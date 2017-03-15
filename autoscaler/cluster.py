import collections
import datetime
import logging
import math
import time

import datadog
import pykube

from azure.mgmt.compute import ComputeManagementClient
from azure.cli.core.commands.client_factory import get_mgmt_service_client

import azure_login
from autoscaler.container_service import ContainerService
import autoscaler.capacity as capacity
from autoscaler.kube import KubePod, KubeNode, KubeResource, KubePodStatus
import autoscaler.utils as utils


# we are interested in all pods, incl. system ones
pykube.Pod.objects.namespace = None

# HACK: https://github.com/kelproject/pykube/issues/29#issuecomment-230026930
import backports.ssl_match_hostname
# Monkey-patch match_hostname with backports's match_hostname, allowing for IP addresses
# XXX: the exception that this might raise is
# backports.ssl_match_hostname.CertificateError
pykube.http.requests.packages.urllib3.connection.match_hostname = backports.ssl_match_hostname.match_hostname

logger = logging.getLogger(__name__)


class ClusterNodeState(object):
    INSTANCE_TERMINATED = 'instance-terminated'
    ASG_MIN_SIZE = 'asg-min-size'
    POD_PENDING = 'pod-pending'
    GRACE_PERIOD = 'grace-period'
    TYPE_GRACE_PERIOD = 'type-grace-period'
    IDLE_SCHEDULABLE = 'idle-schedulable'
    IDLE_UNSCHEDULABLE = 'idle-unschedulable'
    BUSY_UNSCHEDULABLE = 'busy-unschedulable'
    BUSY = 'busy'
    UNDER_UTILIZED_DRAINABLE = 'under-utilized-drainable'
    UNDER_UTILIZED_UNDRAINABLE = 'under-utilized-undrainable'
    LAUNCH_HR_GRACE_PERIOD = 'launch-hr-grace-period'


class Cluster(object):

    # the number of instances that is allowed to be idle
    # this is for keeping some spare capacity around for faster
    # pod scheduling, instead of keeping the cluster at capacity
    # and having to spin up nodes for every job submission
    IDLE_COUNT = 5

    # the utilization threshold under which to consider a node
    # under utilized and drainable
    UTIL_THRESHOLD = 0.3


    def __init__(self, service_principal_app_id, service_principal_secret, service_principal_tenant_id,
                 kubeconfig, idle_threshold, reserve_idle_threshold,
                 instance_init_time, container_service_name, resource_group, notifier,
                 scale_up=True, maintainance=True,
                 datadog_api_key=None,
                 over_provision=5, dry_run=False):
        if kubeconfig:
            # for using locally
            logger.debug('Using kubeconfig %s', kubeconfig)
            self.api = pykube.HTTPClient(
                pykube.KubeConfig.from_file(kubeconfig))
        else:
            # for using on kube
            logger.debug('Using kube service account')
            self.api = pykube.HTTPClient(
                pykube.KubeConfig.from_service_account())

        self._drained = {}

        azure_login.login(
            service_principal_app_id,
            service_principal_secret,
            service_principal,tenant)       
         

        #  Create container service
        self.container_service = ContainerService(
            get_mgmt_service_client(ComputeManagementClient).container_services, 
            container_service_name, 
            resource_group)

        # self.autoscaling_timeouts = autoscaling_groups.AutoScalingTimeouts(
        #     self.session)

        # config
        self.idle_threshold = idle_threshold
        self.instance_init_time = instance_init_time
        self.reserve_idle_threshold = reserve_idle_threshold
        self.over_provision = over_provision

        self.scale_up = scale_up
        self.maintainance = maintainance

        self.notifier = notifier

        if datadog_api_key:
            datadog.initialize(api_key=datadog_api_key)
            logger.info('Datadog initialized')
        self.stats = datadog.ThreadStats()
        self.stats.start()

        self.dry_run = dry_run

    def scale_loop(self):
        """
        runs one loop of scaling to current needs.
        returns True if successfully scaled.
        """
        logger.info("++++++++++++++ Running Scaling Loop ++++++++++++++++")
        try:
            pykube_nodes = pykube.Node.objects(self.api)
            if not pykube_nodes:
                logger.warn(
                    'Failed to list nodes. Please check kube configuration. Terminating scale loop.')
                return False

            all_nodes = map(KubeNode, pykube_nodes)

            #TODO: What is a managed node in this context?
            managed_nodes = [node for node in all_nodes if node.is_managed()]            

            pods = map(KubePod, pykube.Pod.objects(self.api))

            running_or_pending_assigned_pods = [
                p for p in pods if (p.status == KubePodStatus.RUNNING or p.status == KubePodStatus.CONTAINER_CREATING) or (
                    p.status == KubePodStatus.PENDING and p.node_name
                )
            ]

            for node in all_nodes:
                for pod in running_or_pending_assigned_pods:
                    if pod.node_name == node.name:
                        node.count_pod(pod)

            #asgs = self.autoscaling_groups.get_all_groups(all_nodes)

            pods_to_schedule = self.get_pods_to_schedule(pods)

            if self.scale_up:
                logger.info(
                    "++++++++++++++ Scaling Up Begins ++++++++++++++++")
                self.scale(
                    pods_to_schedule, all_nodes, self.container_service)
                logger.info("++++++++++++++ Scaling Up Ends ++++++++++++++++")
            if self.maintainance:
                logger.info(
                    "++++++++++++++ Maintenance Begins ++++++++++++++++")
                self.maintain(
                    managed_nodes,
                    pods_to_schedule, running_or_pending_assigned_pods, self.container_service)
                logger.info("++++++++++++++ Maintenance Ends ++++++++++++++++")

            return True
        except botocore.exceptions.ClientError as e:
            logger.warn(e)
            return False

    def fulfill_pending(self, container_service, unit_capacity, selectors_hash, pods):
        """
        selectors_hash - string repr of selectors
        pods - list of KubePods that are pending
        """

        

        logger.info(
            "========= Scaling for %s ========", selectors_hash)
        logger.debug("pending: %s", pods[:5])

        accounted_pods = dict((p, False) for p in pods)
        num_unaccounted = len(pods)

        new_instance_resources = []
        assigned_pods = []
        for pod, acc in accounted_pods.items():
            if acc or not (unit_capacity - pod.resources).possible:
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
                    unit_capacity - pod.resources)
                assigned_pods.append([pod])

        # new desired # machines = # running nodes + # machines required to fit jobs that don't
        # fit on running nodes. This scaling is conservative but won't
        # create starving
        units_needed = len(new_instance_resources)
        units_needed += self.over_provision
        
        # if self.autoscaling_timeouts.is_timed_out(group):
        #         # if a machine is timed out, it cannot be scaled further
        #         # just account for its current capacity (it may have more
        #         # being launched, but we're being conservative)
        #         unavailable_units = max(
        #             0, units_needed - (group.desired_capacity - group.actual_capacity))
        #     else:
        unavailable_units = max(
            0, units_needed - (container_service.max_size - container_service.actual_capacity))

        units_requested = units_needed - unavailable_units

        logger.debug("units_needed: %s", units_needed)
        logger.debug("units_requested: %s", units_requested)

        new_capacity = container_service.actual_capacity + units_requested
        if not self.dry_run:
            scaled = container_service.scale(new_capacity)
            
            #TODO: reimplement notifications if needed
            # if scaled:
            #     self.notifier.notify_scale(container_service, units_requested, pods)
        else:
            logger.info(
                '[Dry run] Would have scaled up to %s', new_capacity)

        for i in range(min(len(assigned_pods), units_requested)):
            for pod in assigned_pods[i]:
                accounted_pods[pod] = True
                num_unaccounted -= 1

        logger.debug("remaining pending: %s", num_unaccounted)

        if not num_unaccounted:
            break

        if num_unaccounted:
            logger.warn('Failed to scale sufficiently.')
            # self.notifier.notify_failed_to_scale(selectors_hash, pods)

    def get_running_instances_map(self, nodes):      
        # In the AWS version, this func is used to get which nodes are alive
        # Todo: reimplement similar functionality for azure
        return NotImplementedError()


    def get_node_state(self, node, asg, node_pods, pods_to_schedule,
                       running_insts_map, idle_selector_hash):
        #TODO

    def get_pods_to_schedule(self, pods):
        """
        given a list of KubePod objects,
        return a map of (selectors hash -> pods) to be scheduled
        """
        pending_unassigned_pods = [
            p for p in pods
            if p.status == KubePodStatus.PENDING and (not p.node_name)
        ]

        # we only consider a pod to be schedulable if it's pending and
        # unassigned and feasible
        pods_to_schedule = {}
        for pod in pending_unassigned_pods:
            if capacity.is_possible(pod):
                pods_to_schedule.setdefault(
                    utils.selectors_to_hash(pod.selectors), []).append(pod)
            else:
                recommended_capacity = capacity.max_capacity_for_selectors(
                    pod.selectors)
                logger.warn(
                    "Pending pod %s cannot fit %s. "
                    "Please check that requested resource amount is "
                    "consistent with node selectors (recommended max: %s). "
                    "Scheduling skipped." % (pod.name, pod.selectors, recommended_capacity))
                self.notifier.notify_invalid_pod_capacity(
                    pod, recommended_capacity)
        return pods_to_schedule

    def scale(self, pods_to_schedule, all_nodes, container_service):
        """
        scale up logic
        """
        #All the nodes in ACS should be of a single type, so take the capacity of any node as reference
        unit_capacity = all_nodes[:1].capacity


        self.autoscaling_timeouts.refresh_timeouts(asgs, dry_run=self.dry_run)

        #Assume all nodes are alive for now. We will need to implement a way to verify that later on, maybe when VMSS are live?
        cached_live_nodes = all_nodes
       
        pending_pods = {}

        # for each pending & unassigned job, try to fit them on current machines or count requested
        #   resources towards future machines
        for selectors_hash, pods in pods_to_schedule.items():
            for pod in pods:
                fitting = None
                for node in cached_live_nodes:
                    if node.can_fit(pod.resources):
                        fitting = node
                        break
                if fitting is None:
                    pending_pods.setdefault(selectors_hash, []).append(pod)
                    logger.info(
                        "{pod} is pending ({selectors_hash})".format(
                            pod=pod, selectors_hash=selectors_hash))
                else:
                    fitting.count_pod(pod)
                    logger.info("{pod} fits on {node}".format(pod=pod,
                                                              node=fitting))

        # scale nodes to reach the new capacity
        # For now we don't need all of this. We could just scale regardless of selectors in a single batch, since we don't support multiple instance type
        # but keeping the logic in place for future improvments of the platform
        for selectors_hash, pending in pending_pods.items():
            self.fulfill_pending(container_service, unit_capacity, selectors_hash, pending)


    def maintain(self, cached_managed_nodes, running_insts_map,
                 pods_to_schedule, running_or_pending_assigned_pods, asgs):
        """
        maintains running instances:
        - determines if idle nodes should be drained and terminated
        - determines if there are bad nodes in ASGs (did not spin up under
          `instance_init_time` seconds)
        """

        # In our case we cannot decide which node to terminate, we can only terminate the last one.
        # So our maintenance is easy: is the last node underutilized? If yes cordon, drain then kill it.
        # Otherwise we keep everything.
        # If the LB is configured for round-robbin and there is no sticky sessions, long running sessions etc... it should not pose
        # too much issues. Otherwise this solution will not work and will need to wait for k8s to be supported by VMSS.
        # We also have to assume that there is no undrainable and critical pod, otherwise we cannot scale down at all in many cases


        logger.info("++++++++++++++ Maintaining Managed Nodes ++++++++++++++++")

        # for each type of instance, we keep one around for longer
        # in order to speed up job start up time
        idle_selector_hash = collections.Counter()

        pods_by_node = {}
        for p in running_or_pending_assigned_pods:
            pods_by_node.setdefault(p.node_name, []).append(p)

        stats_time = time.time()

        for node in cached_managed_nodes:
            asg = utils.get_group_for_node(asgs, node)
            state = self.get_node_state(
                node, asg, pods_by_node.get(node.name, []), pods_to_schedule,
                running_insts_map, idle_selector_hash)

            logger.info("node: %-*s state: %s" % (75, node, state))
            self.stats.increment(
                'kubernetes.custom.node.state.{}'.format(
                    state.replace('-', '_')),
                timestamp=stats_time)

            # state machine & why doesnt python have case?
            if state in (ClusterNodeState.POD_PENDING, ClusterNodeState.BUSY,
                         ClusterNodeState.GRACE_PERIOD,
                         ClusterNodeState.TYPE_GRACE_PERIOD,
                         ClusterNodeState.ASG_MIN_SIZE,
                         ClusterNodeState.LAUNCH_HR_GRACE_PERIOD):
                # do nothing
                pass
            elif state == ClusterNodeState.UNDER_UTILIZED_DRAINABLE:
                if not self.dry_run:
                    if not asg:
                        logger.warn(
                            'Cannot find ASG for node %s. Not cordoned.', node)
                    else:
                        node.cordon()
                        node.drain(pods_by_node.get(node.name, []),
                                   notifier=self.notifier)
                else:
                    logger.info(
                        '[Dry run] Would have drained and cordoned %s', node)
            elif state == ClusterNodeState.IDLE_SCHEDULABLE:
                if not self.dry_run:
                    if not asg:
                        logger.warn(
                            'Cannot fLAUNCH_HOUR_THRESHOLDind ASG for node %s. Not cordoned.', node)
                    else:
                        node.cordon()
                else:
                    logger.info('[Dry run] Would have cordoned %s', node)
            elif state == ClusterNodeState.BUSY_UNSCHEDULABLE:
                # this is duplicated in original scale logic
                if not self.dry_run:
                    node.uncordon()
                else:
                    logger.info('[Dry run] Would have uncordoned %s', node)
            elif state == ClusterNodeState.IDLE_UNSCHEDULABLE:
                # remove it from asg
                if not self.dry_run:
                    if not asg:
                        logger.warn(
                            'Cannot find ASG for node %s. Not terminated.', node)
                    else:
                        asg.scale_node_in(node)
                else:
                    logger.info('[Dry run] Would have scaled in %s', node)
            elif state == ClusterNodeState.INSTANCE_TERMINATED:
                if not self.dry_run:
                    node.delete()
                else:
                    logger.info('[Dry run] Would have deleted %s', node)
            elif state == ClusterNodeState.UNDER_UTILIZED_UNDRAINABLE:
                # noop for now
                pass
            else:
                raise Exception("Unhandled state: {}".format(state))

        logger.info(
            "++++++++++++++ Maintaining Unmanaged Instances ++++++++++++++++")
        # these are instances that have been running for a while but it's not properly managed
        # i.e. not having registered to kube or not having proper meta data set
        managed_instance_ids = set(
            node.instance_id for node in cached_managed_nodes)
        for asg in asgs:
            unmanaged_instance_ids = list(
                asg.instance_ids - managed_instance_ids)
            if len(unmanaged_instance_ids) != 0:
                unmanaged_running_insts = self.get_running_instances_in_region(
                    asg.region, unmanaged_instance_ids)
                for inst in unmanaged_running_insts:
                    if (datetime.datetime.now(inst.launch_time.tzinfo)
                            - inst.launch_time).seconds >= self.instance_init_time:
                        if not self.dry_run:
                            asg.client.terminate_instance_in_auto_scaling_group(
                                InstanceId=inst.id, ShouldDecrementDesiredCapacity=False)
                            logger.info("terminating unmanaged %s" % inst)
                            self.stats.increment(
                                'kubernetes.custom.node.state.unmanaged',
                                timestamp=stats_time)
                            # TODO: try to delete node from kube as well
                            # in the case where kubelet may have registered but node
                            # labels have not been applied yet, so it appears
                            # unmanaged
                        else:
                            logger.info(
                                '[Dry run] Would have terminated unmanaged %s', inst)
