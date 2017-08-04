from azure.cli.core.util import get_file_json
from azure.cli.core.commands.client_factory import get_mgmt_service_client
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
import logging
import autoscaler.utils as utils
import time

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
        # because of how acs-engine works (offset etc.), the desired capacity is the number of node existing in the pool
        # (even unschedulable) + the number of additional node we need. 
        
        reclaimed = 0
        if (self.actual_capacity + reclaimed) < desired_capacity:
            for node in self.unschedulable_nodes:
                if node.uncordon():
                    # give some time to k8s to assign any pending pod to the newly uncordonned node
                    time.sleep(10)
                    reclaimed += 1
                    # Uncordon only what we need
                    if (self.actual_capacity + reclaimed) == desired_capacity:
                        break
        return (self.actual_capacity + reclaimed)

    def has_node_with_index(self, index):
        for node in self.nodes:
            if node.index == index:
                return True
        return False
    