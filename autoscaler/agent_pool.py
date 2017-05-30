from azure.cli.core.util import get_file_json
from azure.cli.core.commands.client_factory import get_mgmt_service_client
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
import logging
import autoscaler.utils as utils

from autoscaler.capacity import get_capacity_for_instance_type

logger = logging.getLogger(__name__)

class AgentPool(object):

    def __init__(self, pool_name, instance_type, nodes):
        self.name = pool_name
        self.nodes = nodes
        self.unschedulable_nodes = list(filter(lambda n: n.unschedulable, self.nodes))
        self.max_size = 100
        self.instance_type = instance_type

    @property
    def actual_capacity(self):
        return len(self.nodes)
    
    @property
    def unit_capacity(self):
        #Within a pool, every node should have the same capacity
        return get_capacity_for_instance_type(self.instance_type)
    
    def reclaim_unschedulable_nodes(self, new_desired_capacity):
        """
        Try to get the number of schedulable nodes up if we don't have enough before scaling
        """
        desired_capacity = min(self.max_size, new_desired_capacity)
        num_unschedulable = len(self.unschedulable_nodes)
        num_schedulable = self.actual_capacity - num_unschedulable
     
        if num_schedulable < desired_capacity:
            for node in self.unschedulable_nodes:
                if node.uncordon():
                    num_schedulable += 1
                    # Uncordon only what we need
                    if num_schedulable == desired_capacity:
                        break

    def has_node_with_index(self, index):
        for node in self.nodes:
            if node.index == index:
                return True
        return False
    