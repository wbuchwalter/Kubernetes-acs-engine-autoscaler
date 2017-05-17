import collections
import datetime
import logging
import math
import time
import sys
import pykube

import autoscaler.azure_login as azure_login
from autoscaler.container_service import ContainerService
import autoscaler.capacity as capacity
from autoscaler.kube import KubePod, KubeNode, KubeResource, KubePodStatus
import autoscaler.utils as utils
from autoscaler.deployments import Deployments

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
    POD_PENDING = 'pod-pending'
    GRACE_PERIOD = 'grace-period'
    SPARE_AGENT = 'spare-agent'
    IDLE_SCHEDULABLE = 'idle-schedulable'
    IDLE_UNSCHEDULABLE = 'idle-unschedulable'
    BUSY_UNSCHEDULABLE = 'busy-unschedulable'
    BUSY = 'busy'
    UNDER_UTILIZED_DRAINABLE = 'under-utilized-drainable'
    UNDER_UTILIZED_UNDRAINABLE = 'under-utilized-undrainable'


class Cluster(object):

    # the utilization threshold under which to consider a node
    # under utilized and drainable
    UTIL_THRESHOLD = 0.3


    def __init__(self, kubeconfig, template_file, parameters_file, template_file_url, 
                 parameters_file_url, idle_threshold, spare_agents, instance_init_time, 
                 container_service_name, resource_group, notifier,
                 scale_up=True, maintainance=True,
                 over_provision=5, dry_run=False):

        if template_file or template_file_url:
            self.arm_template = utils.get_arm_template(template_file, template_file_url)
            self.arm_parameters = utils.get_arm_parameters(parameters_file, parameters_file_url)

        # config
        self.kubeconfig = kubeconfig
        self._drained = {}
        self.container_service_name = container_service_name
        self.resource_group = resource_group
        self.agent_pools = {}
        self.pools_instance_type = {}
        self.idle_threshold = idle_threshold
        self.instance_init_time = instance_init_time
        self.spare_agents = spare_agents
        self.over_provision = over_provision
        self.scale_up = scale_up
        self.maintainance = maintainance
        self.notifier = notifier
        self.dry_run = dry_run
        self.deployments = Deployments()     
    
    def login(self, service_principal_app_id, service_principal_secret, service_principal_tenant_id):
        azure_login.login(
            service_principal_app_id,
            service_principal_secret,
            service_principal_tenant_id)

        if self.kubeconfig:
            # for using locally
            logger.debug('Using kubeconfig %s', self.kubeconfig)
            self.api = pykube.HTTPClient(
                pykube.KubeConfig.from_file(self.kubeconfig))
        else:
            # for using on kube
            logger.debug('Using kube service account')
            self.api = pykube.HTTPClient(
                pykube.KubeConfig.from_service_account())         
       
    def loop(self, debug):
        """
        runs one loop of scaling to current needs.
        returns True if successfully scaled.
        """
        logger.info("++++ Running Scaling Loop ++++++")

        if debug:
            #In debug mode, we don't want to catch error. Let the app crash explicitly
            return self.loop_logic()
        else:            
            try:
                return self.loop_logic()
            except:
                logger.warn("Unexpected error: {}".format(sys.exc_info()[0]))
                return False

    def loop_logic(self):
        pykube_nodes = pykube.Node.objects(self.api)
        if not pykube_nodes:
            logger.warn(
                'Failed to list nodes. Please check kube configuration. Terminating scale loop.')
            return False

        all_nodes = list(filter(utils.is_agent, map(KubeNode, pykube_nodes))) 

        container_service = ContainerService( 
            resource_group=self.resource_group,
            nodes=all_nodes,            
            deployments=self.deployments,
            container_service_name=self.container_service_name,
            arm_template=self.arm_template,
            arm_parameters=self.arm_parameters,
            dry_run=self.dry_run,
            over_provision=self.over_provision)

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
        pods_to_schedule = self.get_pods_to_schedule(pods, container_service.agent_pools)
        logger.info("Pods to schedule: {}".format(len(pods_to_schedule)))  
        

        if self.scale_up:
            logger.info("++++ Scaling Up Begins ++++++")
            self.scale(pods_to_schedule, all_nodes, container_service)
            logger.info("++++ Scaling Up Ends ++++++")
        if self.maintainance:
            logger.info("++++ Maintenance Begins ++++++")
            self.maintain(pods_to_schedule, running_or_pending_assigned_pods, container_service)
            logger.info("++++ Maintenance Ends ++++++")

        return True

    def get_pending_pods(self, pods, nodes):
        pending_pods = []
        # for each pending & unassigned job, try to fit them on current machines or count requested
        #   resources towards future machines
        for pod in pods: 
            fitting = None
            for node in nodes:
                if node.can_fit(pod.resources):
                    fitting = node
                    break
            if fitting is None:
                pending_pods.append(pod)                
            else:
                fitting.count_pod(pod)
                logger.info("{pod} fits on {node}".format(pod=pod,
                                                            node=fitting))
        logger.info("Pending pods: {}".format(len(pending_pods)))

        return pending_pods

    def scale(self, pods_to_schedule, nodes, container_service):
        """
        scale up logic
        """
        logger.info("Nodes: {}".format(len(nodes)))
        logger.info("To schedule: {}".format(len(pods_to_schedule)))  

        pending_pods = self.get_pending_pods(pods_to_schedule, nodes)
        if len(pending_pods) > 0:
            container_service.fulfill_pending(pending_pods)

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
        
        #TODO: Fix this kube-proxy issue, see https://github.com/openai/kubernetes-ec2-autoscaler/issues/23
        undrainable_list = [p for p in node_pods if not (p.is_drainable() or 'kube-proxy' in p.name)]
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
        else:
            if node.unschedulable:
                state = ClusterNodeState.IDLE_UNSCHEDULABLE
            else:
                state = ClusterNodeState.IDLE_SCHEDULABLE

        return state

    def get_pods_to_schedule(self, pods, agent_pools):
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
            if capacity.is_possible(pod, agent_pools):
                pods_to_schedule.append(pod)
            else:                
                logger.warn(
                    "Pending pod %s cannot fit. "
                    "Please check that requested resource amount is "
                    "consistent with node size."
                    "Scheduling skipped." % (pod.name))            
                
        return pods_to_schedule
    
    

    def maintain(self, pods_to_schedule, running_or_pending_assigned_pods, container_service):
        """
        maintains running instances:
        - determines if idle nodes should be drained and terminated
        """

        # In our case we cannot decide which node to terminate, we can only terminate the last one.
        # So our maintenance is easy: is the last node underutilized? If yes cordon, drain then kill it.
        # Otherwise we keep everything.
        # If the LB is configured for round-robbin and there is no sticky sessions, long running sessions etc... it should not pose
        # too much issues. Otherwise this solution will not work and will need to wait for k8s to be supported by VMSS.
        # We also have to assume that there is no undrainable and critical pod, otherwise we cannot scale down at all in many cases

        logger.info("++++ Maintaining Nodes ++++++")        

        pods_by_node = {}
        for p in running_or_pending_assigned_pods:
            pods_by_node.setdefault(p.node_name, []).append(p)

        stats_time = time.time()
        
        trim_map = {}

        for pool in container_service.agent_pools:
            #Since we can only 'trim' nodes from the end with ACS, start by the end, and so how many we should trim
            #break once we find a node that should node be deleted or cordoned
            nodes_to_trim = 0
            #flag used to notify that we don't want to delete/drain/cordon further, but we still want to display the state of each node
            #this only used for ACS, as we can delete any node we want in acs-engine
            trim_ended = False 
            #maximum nomber of nodes we can drain without hitting our spare capacity
            max_nodes_to_drain = pool.actual_capacity - self.spare_agents
            
            nodes = pool.nodes.copy()
            nodes.reverse()
            
            for node in nodes:
                state = self.get_node_state(node, pods_by_node.get(node.name, []), pods_to_schedule)
                if state == ClusterNodeState.UNDER_UTILIZED_DRAINABLE:
                    #For ACS, spare agents are always the older nodes                   
                    if not container_service.is_acs_engine and node.instance_index < self.spare_agents:
                        state = ClusterNodeState.SPARE_AGENT
                    elif container_service.is_acs_engine and max_nodes_to_drain == 0:
                        state = ClusterNodeState.SPARE_AGENT                
              
                logger.info("node: %-*s state: %s" % (75, node, state))

                #With ACS, if we don't want to break the SLA, we can only kill nodes starting by the most recent
                #With acs-engine, we can directly delete any node using Azure API
                if trim_ended and not container_service.is_acs_engine:
                    continue

                # state machine & why doesnt python have case?
                if state in (ClusterNodeState.POD_PENDING, ClusterNodeState.BUSY,
                            ClusterNodeState.SPARE_AGENT):                       
                    # do nothing
                    trim_ended = True
                elif state == ClusterNodeState.UNDER_UTILIZED_DRAINABLE and (not trim_ended or container_service.is_acs_engine):
                    if not self.dry_run:
                        node.cordon()
                        node.drain(pods_by_node.get(node.name, []),
                                    notifier=self.notifier)
                    else:
                        logger.info(
                            '[Dry run] Would have drained and cordoned %s', node)
                elif state == ClusterNodeState.IDLE_SCHEDULABLE:
                    if not self.dry_run:
                        node.cordon()
                    else:
                        logger.info('[Dry run] Would have cordoned %s', node)
                elif state == ClusterNodeState.BUSY_UNSCHEDULABLE:
                    # this is duplicated in original scale logic
                    if not self.dry_run:
                        node.uncordon()
                    else:
                        logger.info('[Dry run] Would have uncordoned %s', node)
                    trim_ended = True
                elif state == ClusterNodeState.IDLE_UNSCHEDULABLE:
                    if not self.dry_run:
                        nodes_to_trim += 1
                        if container_service.is_acs_engine:
                            container_service.delete_node(pool, node)
                    else:
                        logger.info('[Dry run] Would have scaled in %s', node)
                elif state == ClusterNodeState.UNDER_UTILIZED_UNDRAINABLE:
                    # noop for now
                    trim_ended = True
                else:
                    raise Exception("Unhandled state: {}".format(state))
            trim_map[pool.name] = nodes_to_trim
        
        if not container_service.is_acs_engine and len(list(filter(lambda x: trim_map[x] > 0, trim_map))) > 0:
            container_service.scale_down(trim_map, self.dry_run)