from azure.cli.core._util import get_file_json
from azure.cli.core.commands.client_factory import get_mgmt_service_client
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
import time
import logging
import autoscaler.utils as utils
from autoscaler.agent_pool import AgentPool


logger = logging.getLogger(__name__)

class ContainerService(object):

    def __init__(self, resource_group, nodes, container_service_name, deployments):
        self.resource_group_name = resource_group
        self.deployments = deployments
        self.is_acs_engine = True      
        if container_service_name:
            self.container_service_name = container_service_name        
            self.is_acs_engine = False
            self.acs_client = get_mgmt_service_client(ComputeManagementClient).container_services
            self.instance = self.acs_client.get(resource_group, container_service_name)   
        
        #ACS support up to 100 agents today
        #TODO: how to handle case where cluster has 0 node? How to get unit capacity?
        self.max_agent_pool_size = 100
        self.agent_pools = self.get_agent_pools(nodes)       
        
    def get_agent_pools(self, nodes):
        pools = {}
        for node in nodes:            
            pool_name = utils.get_pool_name(node)
            pools.setdefault(pool_name, []).append(node)
        
        agent_pools = []
        for pool_name in pools:
            agent_pools.append(AgentPool(pool_name, pools[pool_name]))

        return agent_pools

    def scale_down(self, trim_map, dry_run):
        """
        Scale down each agent pool (most recent nodes will be deleted first)
        """
        new_pool_sizes = {}
        for pool in self.agent_pools:
            new_agent_count = pool.actual_capacity - trim_map[pool.name]
            if  new_agent_count <= 0:            
                raise Exception("Tried to scale down pool {} to less than 1 agent".format(pool.name))
            
            logger.info("Scaling down pool {} by {} agents".format(pool.name, trim_map[pool.name]))
            new_pool_sizes[pool.name] = new_agent_count

        self.scale_pools(new_pool_sizes, dry_run)

    def scale_pools(self, new_pool_sizes, dry_run):        
        has_changes = False
        for pool in self.agent_pools:
            new_size = new_pool_sizes[pool.name]            
            new_pool_sizes[pool.name] = min(pool.max_size, new_size)
            if new_pool_sizes[pool.name] == pool.actual_capacity:
                logger.info("Pool '{}' already at desired capacity ({})".format(pool.name, pool.actual_capacity))
                continue
            has_changes = True                

            if not dry_run:
                if new_size > pool.actual_capacity:
                    pool.reclaim_unschedulable_nodes(new_size)
            else:
                logger.info("[Dry run] Would have scaled pool '{}' to {} agent(s) (currently at {})".format(pool.name, new_size, pool.actual_capacity))
        
        if not dry_run and has_changes:        
            if not self.is_acs_engine:
                for pool in self.agent_pools:
                    self.deployments.deploy(lambda: self.set_desired_acs_agent_pool_capacity(new_pool_sizes[pool.name]), new_pool_sizes)
                    # self.set_desired_acs_agent_pool_capacity(new_pool_sizes[pool.name])
            else:
                self.deployments.deploy(lambda: self.deploy_pools(new_pool_sizes), new_pool_sizes)                
                

    def set_desired_acs_agent_pool_capacity(self, new_desired_capacity):
        """
        sets the desired capacity of the underlying ASG directly.
        note that this is for internal control.
        for scaling purposes, please use scale() instead.
        """

        #We only support one agent pool on ACS
        self.instance.agent_pool_profiles[0].count = new_desired_capacity         

        # null out the service principal because otherwise validation complains
        self.instance.service_principal_profile = None
        self.desired_agent_pool_capacity = new_desired_capacity
        return self.acs_client.create_or_update(self.resource_group_name, self.container_service_name, self.instance)             

    
    def deploy_pools(self, new_pool_sizes):
        print('deploying')
        from azure.mgmt.resource.resources.models import DeploymentProperties, TemplateLink   
        parameters = get_file_json('./azuredeploy.parameters.json')
        parameters = parameters.get('parameters', parameters)       
        
        for pool_name in new_pool_sizes:
            parameters[pool_name + 'Count'] = {'value': new_pool_sizes[pool_name]}
            logger.info('Requested size for {}: {}'.format(pool_name, new_pool_sizes[pool_name]))
        
        template = get_file_json('./azuredeploy.json')    
        properties = DeploymentProperties(template=template, template_link=None,
                                        parameters=parameters, mode='complete')

        smc = get_mgmt_service_client(ResourceManagementClient)
        return smc.deployments.create_or_update(self.resource_group_name, "autoscale", properties, raw=False)
  