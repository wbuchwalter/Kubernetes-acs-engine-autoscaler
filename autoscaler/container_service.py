import logging
import autoscaler.utils as utils

logger = logging.getLogger(__name__)

class ContainerService(object):

    def __init__(self, acs_client, container_service_name, resource_group, nodes):
        self.resource_group_name = resource_group
        self.container_service_name = container_service_name
        self.acs_client = acs_client 
        self.instance = self.acs_client.get(resource_group, container_service_name)
        self.nodes = nodes 

        #ACS support up to 100 agents today
        #WB: how to handle case where cluster has 0 node? How to get unit capacity?
        self.max_size = 100
        self.desired_capacity = len(self.nodes)
        self.unschedulable_nodes = list(filter(lambda n: n.unschedulable, self.nodes))
        self.master_count = utils.count_master(self.nodes)

    def capacity(self):
        return NotImplementedException()

    @property
    def actual_capacity(self):
        return len(self.nodes)

    def scale_agent_pool(self, new_desired_capacity):
        """
        scales the container service to the new desired capacity.
        returns True if desired capacity has been increased as a result.
        """

        desired_capacity = min(self.max_size, new_desired_capacity)
        num_unschedulable = len(self.unschedulable_nodes)
        num_schedulable = self.actual_capacity - num_unschedulable

        logger.info("Desired {}, currently at {}".format(
            desired_capacity, self.desired_capacity))
        logger.info("Kube node: {} schedulable, {} unschedulable".format(
            num_schedulable, num_unschedulable))

        # Try to get the number of schedulable nodes up if we don't have enough, regardless of whether
        # ACS's capacity is already at the same as the desired.
        if num_schedulable < desired_capacity:
            for node in self.unschedulable_nodes:
                if node.uncordon():
                    num_schedulable += 1
                    # Uncordon only what we need
                    if num_schedulable == desired_capacity:
                        break

        if self.desired_capacity != desired_capacity:
            if self.desired_capacity == self.max_size:
                logger.info("Desired same as max, desired: {}, schedulable: {}".format(
                    self.desired_capacity, num_schedulable))
                return False

            scale_up = self.desired_capacity < desired_capacity
            if scale_up:
                # should have gotten our num_schedulable to highest value possible
                # actually need to grow.
                self.set_desired_agent_capacity(desired_capacity)
                return True

        logger.info("Doing nothing: desired_capacity correctly set: {}, schedulable: {}".format(self.container_service_name, num_schedulable))
        return False


    def set_desired_agent_capacity(self, new_desired_capacity):
        """
        sets the desired capacity of the underlying ASG directly.
        note that this is for internal control.
        for scaling purposes, please use scale() instead.
        """

        #We only support one agent pool on ACS
        self.instance.agent_pool_profiles[0].count = new_desired_capacity

        logger.info("ACS: {} new agent pool size: {}".format(new_desired_capacity))


        # null out the service principal because otherwise validation complains
        self.instance.service_principal_profile = None

        self.acs_client.create_or_update(self.resource_group_name, self.container_service_name, self.instance)

        self.desired_capacity = new_desired_capacity

    def scale_down(self, nb_to_trim):
        """
        Scale down the agent pool by nb_to_trim (most recent nodes will be deleted)
        """
        
        if nb_to_trim == 0:
            return False
        new_agent_count = len(nodes) - self.master_count - nb_to_trim
        if  new_agent_count <= 0:            
            logger.error("Tried to delete master nodes or scale down to less than 1 agent")
            return False
        
        logger.info("Scaling down by {} agents".format(nb_to_trim))
        
        self.set_desired_capacity(new_agent_count)
        return True

  