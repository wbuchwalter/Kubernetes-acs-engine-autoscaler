import collections
import datetime
import logging
import math
import time
import sys

import datadog
import pykube

from azure.mgmt.compute import ComputeManagementClient
from azure.cli.core.commands.client_factory import get_mgmt_service_client

import autoscaler.azure_login as azure_login
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

        self.container_service_name = container_service_name
        self.resource_group = resource_group
        #Container Service instance type. Currently ACS only supports one agent pool 
        #so all nodes are of the same type
        self.cs_instance_type = {}

        azure_login.login(
            service_principal_app_id,
            service_principal_secret,
            service_principal_tenant_id)       
         

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

    def scale_loop(self, debug):
        """
        runs one loop of scaling to current needs.
        returns True if successfully scaled.
        """
        logger.info("++++++++++++++ Running Scaling Loop ++++++++++++++++")

        if debug:
            return self.scale_loop_logic()
        else:            
            try:
                return self.scale_loop_logic()
            except:
                logger.warn("Unexpected error: {}".format(sys.exc_info()[0]))
                return False

    def scale_loop_logic(self):
        pykube_nodes = pykube.Node.objects(self.api)
        if not pykube_nodes:
            logger.warn(
                'Failed to list nodes. Please check kube configuration. Terminating scale loop.')
            return False

        all_nodes = utils.order_nodes(list(map(KubeNode, pykube_nodes)))
        #ACS only has support for one agent pool at the moment,
        #so take any agent as reference
        self.cs_instance_type = all_nodes[0].instance_type           

        pods = list(map(KubePod, pykube.Pod.objects(self.api)))
        
        running_or_pending_assigned_pods = [
            p for p in pods if (p.status == KubePodStatus.RUNNING or p.status == KubePodStatus.CONTAINER_CREATING) or (
                p.status == KubePodStatus.PENDING and p.node_name
            )
        ]
        
        for node in all_nodes:
            for pod in running_or_pending_assigned_pods:
                if pod.node_name == node.name:
                    node.count_pod(pod)  
        pods_to_schedule = self.get_pods_to_schedule(pods)
        logger.info("Pods to schedule: {}".format(len(pods_to_schedule)))        
        
        container_service = ContainerService(
            get_mgmt_service_client(ComputeManagementClient).container_services, 
            self.container_service_name, 
            self.resource_group,
            all_nodes)

        if self.scale_up:
            logger.info(
                "++++++++++++++ Scaling Up Begins ++++++++++++++++")
            self.scale(
                pods_to_schedule, all_nodes, container_service)
            logger.info("++++++++++++++ Scaling Up Ends ++++++++++++++++")
        # if self.maintainance:
        #     logger.info(
        #         "++++++++++++++ Maintenance Begins ++++++++++++++++")
        #     self.maintain(
        #         all_nodes,
        #         pods_to_schedule, running_or_pending_assigned_pods, self.container_service)
        #     logger.info("++++++++++++++ Maintenance Ends ++++++++++++++++")

        return True

    def fulfill_pending(self, container_service, unit_capacity, pods):
        """
        selectors_hash - string repr of selectors
        pods - list of KubePods that are pending
        """

        logger.info("========= Scaling for %s pods ========", len(pods))

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

        if num_unaccounted:
            logger.warn('Failed to scale sufficiently.')
            # self.notifier.notify_failed_to_scale(selectors_hash, pods) 

    def get_node_state(self, node, asg, node_pods, pods_to_schedule,
                       running_insts_map, idle_selector_hash):
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
        pending_list = []
        for pods in pods_to_schedule.values():
            for pod in pods:
                if node.is_match(pod):
                    pending_list.append(pod)
        # we consider a node to be busy if it's running any non-DaemonSet pods
        # TODO: we can be a bit more aggressive in killing pods that are
        # replicated
        busy_list = [p for p in node_pods if not p.is_mirrored()]
        undrainable_list = [p for p in node_pods if not p.is_drainable()]
        utilization = sum((p.resources for p in busy_list), KubeResource())
        under_utilized = (self.UTIL_THRESHOLD *
                          node.capacity - utilization).possible
        drainable = not undrainable_list

        # maybe_inst = running_insts_map.get(node.instance_id)
        # if maybe_inst:
        #     age = (datetime.datetime.now(maybe_inst.launch_time.tzinfo)
        #            - maybe_inst.launch_time).seconds
        #     launch_hour_offset = age % 3600
        # else:
        #      = None

        # instance_type = utils.selectors_to_hash(
        #     asg.selectors) if asg else node.instance_type

        spare_capacity = (instance_type and self.type_idle_threshold and
                               idle_selector_hash[instance_type] < self.TYPE_IDLE_COUNT)

        if maybe_inst is None:
            state = ClusterNodeState.INSTANCE_TERMINATED
        elif asg and len(asg.nodes) <= asg.min_size:
            state = ClusterNodeState.ASG_MIN_SIZE
        elif busy_list and not under_utilized:
            if node.unschedulable:
                state = ClusterNodeState.BUSY_UNSCHEDULABLE
            else:
                state = ClusterNodeState.BUSY
        elif pending_list and not node.unschedulable:
            state = ClusterNodeState.POD_PENDING
        # elif launch_hour_offset < self.LAUNCH_HOUR_THRESHOLD and not node.unschedulable:
        #     state = ClusterNodeState.LAUNCH_HR_GRACE_PERIOD
        elif (not type_spare_capacity and age <= self.idle_threshold) and not node.unschedulable:
            # there is already an instance of this type sitting idle
            # so we use the regular idle threshold for the grace period
            state = ClusterNodeState.GRACE_PERIOD
        elif (type_spare_capacity and age <= self.type_idle_threshold) and not node.unschedulable:
            # we don't have an instance of this type yet!
            # use the type idle threshold for the grace period
            # and mark the type as seen
            idle_selector_hash[instance_type] += 1
            state = ClusterNodeState.TYPE_GRACE_PERIOD
        elif under_utilized and (busy_list or not node.unschedulable):
            # nodes that are under utilized (but not completely idle)
            # have their own states to tell if we should drain them
            # for better binpacking or not
            if drainable:
                state = ClusterNodeState.UNDER_UTILIZED_DRAINABLE
            else:
                state = ClusterNodeState.UNDER_UTILIZED_UNDRAINABLE
        else:
            if node.unschedulable:
                state = ClusterNodeState.IDLE_UNSCHEDULABLE
            else:
                state = ClusterNodeState.IDLE_SCHEDULABLE

        return state

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
        pods_to_schedule = []
        for pod in pending_unassigned_pods:
            if capacity.is_possible(pod, self.cs_instance_type):
                pods_to_schedule.append(pod)
            else:                
                logger.warn(
                    "Pending pod %s cannot fit. "
                    "Please check that requested resource amount is "
                    "consistent with node size."
                    "Scheduling skipped." % (pod.name))            
                
        return pods_to_schedule
    
    def scale(self, pods_to_schedule, all_nodes, container_service):
        """
        scale up logic
        """
        logger.info("Nodes: {}".format(len(all_nodes)))
        logger.info("To schedule: {}".format(len(pods_to_schedule)))        

        #All the nodes in ACS should be of a single type, so take the capacity of any node as reference
        unit_capacity = all_nodes[0].capacity
        
        # ???
        #self.autoscaling_timeouts.refresh_timeouts(asgs, dry_run=self.dry_run)

        #Assume all nodes are alive for now. We will need to implement a way to verify that later on, maybe when VMSS are live?
        cached_live_nodes = all_nodes
       
        pending_pods = []
        
        # for each pending & unassigned job, try to fit them on current machines or count requested
        #   resources towards future machines
        for pod in pods_to_schedule: 
            print(pod)         
            fitting = None
            for node in cached_live_nodes:
                if node.can_fit(pod.resources):
                    fitting = node
                    break
            if fitting is None:
                pending_pods.append(pod)
                logger.info("{} is pending".format(pod))
            else:
                fitting.count_pod(pod)
                logger.info("{pod} fits on {node}".format(pod=pod,
                                                            node=fitting))


        logger.info("Pending: {}".format(len(pending_pods)))    
        self.fulfill_pending(container_service, unit_capacity, pending_pods)


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
