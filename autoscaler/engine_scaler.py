import time
import os
import logging
import json
import uuid
from threading import Thread, Lock
from copy import deepcopy

import autoscaler.utils as utils
from autoscaler.agent_pool import AgentPool
from autoscaler.scaler import Scaler, ClusterNodeState
from autoscaler.template_processing import prepare_template_for_scale_out
from autoscaler.azure_api import delete_resources_for_node, create_deployment

logger = logging.getLogger(__name__)


class EngineScaler(Scaler):

    def __init__(
            self, resource_group, nodes,
            over_provision, spare_count, idle_threshold, dry_run,
            deployments, arm_template, arm_parameters, ignore_pools, notifier):

        Scaler.__init__(
            self, resource_group, nodes, over_provision,
            spare_count, idle_threshold, dry_run, deployments, notifier)

        self.arm_parameters = arm_parameters
        self.arm_template = arm_template
        for pool_name in ignore_pools.split(','):
            self.ignored_pool_names[pool_name] = True
        self.agent_pools, self.scalable_pools = self.get_agent_pools(nodes)

    def get_agent_pools(self, nodes):
        params = self.arm_parameters
        pools = {}
        for param in params:
            if param.endswith('VMSize') and param != 'masterVMSize':
                pool_name = param[:-6]
                pools.setdefault(
                    pool_name, {'size': params[param]['value'], 'nodes': []})
        for node in nodes:
            pool_name = utils.get_pool_name(node)
            pools[pool_name]['nodes'].append(node)

        agent_pools = []
        scalable_pools = []
        for pool_name in pools:
            pool_info = pools[pool_name]
            pool = AgentPool(pool_name, pool_info['size'], pool_info['nodes'])
            agent_pools.append(pool)
            if not pool_name in self.ignored_pool_names:
                scalable_pools.append(pool)

        return agent_pools, scalable_pools

    def delete_node(self, pool, node, lock):
        pool_sizes = {}
        with lock:
            for pool in self.agent_pools:
                pool_sizes[pool.name] = pool.actual_capacity
            pool_sizes[pool.name] = pool.actual_capacity - 1
            self.deployments.requested_pool_sizes = pool_sizes

        delete_resources_for_node(node, self.resource_group_name)

    def scale_pools(self, new_pool_sizes):
        has_changes = False
        for pool in self.scalable_pools:
            new_size = new_pool_sizes[pool.name]
            new_pool_sizes[pool.name] = min(pool.max_size, new_size)
            if new_pool_sizes[pool.name] == pool.actual_capacity:
                logger.info("Pool '{}' already at desired capacity ({})".format(
                    pool.name, pool.actual_capacity))
                continue
            has_changes = True

            if not self.dry_run:
                if new_size > pool.actual_capacity:
                    pool.reclaim_unschedulable_nodes(new_size)
            else:
                logger.info("[Dry run] Would have scaled pool '{}' to {} agent(s) (currently at {})".format(
                    pool.name, new_size, pool.actual_capacity))

        if not self.dry_run and has_changes:
            self.deployments.deploy(lambda: self.deploy_pools(
                new_pool_sizes), new_pool_sizes)

    def deploy_pools(self, new_pool_sizes):
        from azure.mgmt.resource.resources.models import DeploymentProperties, TemplateLink
        for pool in self.scalable_pools:
            if new_pool_sizes[pool.name] == 0:
                # This is required as 0 is not an accepted value for the Count parameter,
                # but setting the offset to 1 actually prevent the deployment
                # from changing anything
                self.arm_parameters[pool.name +
                                    'Count'] = {'value': 1}
                self.arm_parameters[pool.name +
                                    'Offset'] = {'value': 1}
            else:
                # We don't need to set the offset parameter as we are directly specifying each
                # resource in the template instead of using Count func
                self.arm_parameters[pool.name +
                                    'Count'] = {'value': new_pool_sizes[pool.name]}

        template = prepare_template_for_scale_out(
            self.arm_template, self.agent_pools, new_pool_sizes)

        properties = DeploymentProperties(template=template, template_link=None,
                                          parameters=self.arm_parameters, mode='incremental')

        deployment_id = str(uuid.uuid4()).split('-')[0]
        deployment_name = "autoscaler-deployment-{}".format(deployment_id)       
        logger.info('Deployment {} started...'.format(deployment_name))
        return create_deployment(self.resource_group_name,
                                    deployment_name,
                                    properties)

    def maintain(self, pods_to_schedule, running_or_pending_assigned_pods):
        """
        maintains running instances:
        - determines if idle nodes should be drained and terminated
        """

        logger.info("++++ Maintaining Nodes ++++++")

        delete_queue = []
        pods_by_node = {}
        for p in running_or_pending_assigned_pods:
            pods_by_node.setdefault(p.node_name, []).append(p)

        for pool in self.scalable_pools:
                # maximum nomber of nodes we can drain without hiting our spare
                # capacity
            max_nodes_to_drain = pool.actual_capacity - len(pool.unschedulable_nodes) - self.spare_count

            for node in pool.nodes:
                state = self.get_node_state(
                    node, pods_by_node.get(node.name, []), pods_to_schedule)

                if state == ClusterNodeState.UNDER_UTILIZED_DRAINABLE:
                    if max_nodes_to_drain == 0:
                        state = ClusterNodeState.SPARE_AGENT

                logger.info("node: %-*s state: %s" % (75, node, state))

                # state machine & why doesnt python have case?
                if state in (ClusterNodeState.POD_PENDING, ClusterNodeState.BUSY,
                             ClusterNodeState.SPARE_AGENT, ClusterNodeState.GRACE_PERIOD):
                    # do nothing
                    pass
                elif state == ClusterNodeState.UNDER_UTILIZED_DRAINABLE:
                    if not self.dry_run:
                        node.cordon()
                        notifier = self.notifier or None
                        node.drain(pods_by_node.get(node.name, []),
                                   notifier)
                        max_nodes_to_drain -= 1
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
                elif state == ClusterNodeState.IDLE_UNSCHEDULABLE:
                    if not self.dry_run:
                        delete_queue.append({'node': node, 'pool': pool})
                    else:
                        logger.info('[Dry run] Would have scaled in %s', node)
                elif state == ClusterNodeState.UNDER_UTILIZED_UNDRAINABLE:
                    pass
                else:
                    raise Exception("Unhandled state: {}".format(state))

        threads = []
        lock = Lock()
        for item in delete_queue:
            t = Thread(target=self.delete_node,
                       args=(item['pool'], item['node'], lock, ))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
