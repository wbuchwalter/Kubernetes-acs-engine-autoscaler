from azure.cli.core.commands.client_factory import get_mgmt_service_client
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlockBlobService
import time
import logging
import autoscaler.utils as utils
from autoscaler.agent_pool import AgentPool
from autoscaler.kube import KubeResource
import autoscaler.capacity as capacity

logger = logging.getLogger(__name__)


class ClusterNodeState(object):
    INSTANCE_TERMINATED = 'instance-terminated'
    POD_PENDING = 'pod-pending'
    GRACE_PERIOD = 'grace-period'
    SPARE_AGENT = 'spare-agent'
    IDLE_SCHEDULABLE = 'idle-schedulable'
    IDLE_UNSCHEDULABLE = 'idle-unschedulable'
    BUSY_UNSCHEDULABLE = 'busy-unschedulable'
    BUSY = 'busy'
    UNDER_UTILIZED_DRAINABLE = 'under-utilized-drainable'
    UNDER_UTILIZED_UNDRAINABLE = 'under-utilized-undrainable'


class Scaler(object):

    # the utilization threshold under which to consider a node
    # under utilized and drainable
    UTIL_THRESHOLD = 0.3

    def __init__(self, resource_group, nodes, over_provision, spare_count, dry_run, deployments):
        self.resource_group_name = resource_group
        self.over_provision = over_provision
        self.spare_count = spare_count
        self.dry_run = dry_run
        self.deployments = deployments

        # ACS support up to 100 agents today
        # TODO: how to handle case where cluster has 0 node? How to get unit
        # capacity?
        self.max_agent_pool_size = 100
        self.agent_pools = None
        self.scalable_pools = None
        self.ignored_pool_names = {}
    
    def get_agent_pools(self, nodes):
        raise NotImplementedError()

    def scale_pools(self, pool_sizes):
        raise NotImplementedError()

    def get_node_state(self, node, node_pods, pods_to_schedule):
        """
        returns the ClusterNodeState for the given node
        params:
        node - KubeNode object
        asg - AutoScalingGroup object that this node belongs in. can be None.
        node_pods - list of KubePods assigned to this node
        pods_to_schedule - list of all pending pods
        running_inst_map - map of all (instance_id -> ec2.Instance object)
        idle_selector_hash - current map of idle nodes by type. may be modified.
        """

        # we consider a node to be busy if it's running any non-DaemonSet pods
        # TODO: we can be a bit more aggressive in killing pods that are
        # replicated
        busy_list = [p for p in node_pods if not p.is_mirrored()]

        # TODO: Fix this kube-proxy issue, see
        # https://github.com/openai/kubernetes-ec2-autoscaler/issues/23
        undrainable_list = [p for p in node_pods if not (
            p.is_drainable() or 'kube-proxy' in p.name)]

        utilization = sum((p.resources for p in busy_list), KubeResource())
        under_utilized = (self.UTIL_THRESHOLD *
                          node.capacity - utilization).possible
        drainable = not undrainable_list

        if busy_list and not under_utilized:
            if node.unschedulable:
                state = ClusterNodeState.BUSY_UNSCHEDULABLE
            else:
                state = ClusterNodeState.BUSY
        elif pods_to_schedule and not node.unschedulable:
            state = ClusterNodeState.POD_PENDING
        # elif is_spare_agent:
        #     state = ClusterNodeState.SPARE_AGENT
        elif under_utilized and (busy_list or not node.unschedulable):
            if drainable:
                state = ClusterNodeState.UNDER_UTILIZED_DRAINABLE
            else:
                state = ClusterNodeState.UNDER_UTILIZED_UNDRAINABLE
                # logger.info('Undrainable pods: {}'.format(
                #         undrainable_list))
        else:
            if node.unschedulable:
                state = ClusterNodeState.IDLE_UNSCHEDULABLE
            else:
                state = ClusterNodeState.IDLE_SCHEDULABLE

        return state

    # Calculate the number of new VMs needed to accomodate all pending pods
    def fulfill_pending(self, pods):
        logger.info("====Scaling for %s pods ====", len(pods))
        accounted_pods = dict((p, False) for p in pods)
        num_unaccounted = len(pods)
        new_pool_sizes = {}
        ordered_pools = capacity.order_by_cost_asc(self.agent_pools)
        for pool in ordered_pools:
            new_pool_sizes[pool.name] = pool.actual_capacity

            if pool.name in self.ignored_pool_names or not num_unaccounted:
                continue

            new_instance_resources = []
            assigned_pods = []
            for pod, acc in accounted_pods.items():
                if acc or not (pool.unit_capacity - pod.resources).possible:
                    continue

                found_fit = False
                for i, instance in enumerate(new_instance_resources):
                    if (instance - pod.resources).possible:
                        new_instance_resources[i] = instance - pod.resources
                        assigned_pods[i].append(pod)
                        found_fit = True
                        break
                if not found_fit:
                    new_instance_resources.append(
                        pool.unit_capacity - pod.resources)
                    assigned_pods.append([pod])

            # new desired # machines = # running nodes + # machines required to fit jobs that don't
            # fit on running nodes. This scaling is conservative but won't
            # create starving
            units_needed = len(new_instance_resources)
            units_needed += self.over_provision

            unavailable_units = max(
                0, units_needed - (pool.max_size - pool.actual_capacity))

            units_requested = units_needed - unavailable_units

            logger.debug("units_needed: %s", units_needed)
            logger.debug("units_requested: %s", units_requested)

            new_capacity = pool.actual_capacity + units_requested
            logger.debug('{} actual capacity: {} , units requested: {}'.format(
                pool.name, pool.actual_capacity, units_requested))
            new_pool_sizes[pool.name] = new_capacity

            logger.info("New capacity requested for pool {}: {} agents (current capacity: {} agents)".format(
                pool.name, new_capacity, pool.actual_capacity))

            for i in range(min(len(assigned_pods), units_requested)):
                for pod in assigned_pods[i]:
                    accounted_pods[pod] = True
                    num_unaccounted -= 1

            logger.debug("remaining pending: %s", num_unaccounted)

        if num_unaccounted:
            logger.warn('Failed to scale sufficiently.')
            # self.notifier.notify_failed_to_scale(selectors_hash, pods)
        self.scale_pools(new_pool_sizes)
