

class ContainerService(object):

  def __init__(acs_client, container_service_name, resource_group):
    self.resource_group_name = resource_group
    self.container_service_name = container_service_name
    self.acs_client = acs_client 
    self.instance = client.get(resource_group, container_service_name)
    self.max_size = 100
    self.min_size = 0 #based on the autoscaler --min arg and VM sku

    self.nodes = 0 #query kube to know
    self.actual_capacity = 0 # num of nodes * CPU per node
    self.desired_capacity = 0 # desired capacity might be different if we are for example in a scale up process

    #keep this list around for future uses, but it should always be empty
    self.unschedulable_nodes = filter(lambda n: n.unschedulable, self.nodes)

  
  def capacity(self):
    return NotImplementedException()


  def scale(self, new_desired_capacity):
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
    # group's capacity is already at the same as the desired.
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
            self.set_desired_capacity(desired_capacity)
            return True

    logger.info("Doing nothing: desired_capacity correctly set: {}, schedulable: {}".format(
        self.name, num_schedulable))set_desired_capacity
    return False


  def set_desired_capacity(self, new_desired_capacity):
    """
    sets the desired capacity of the underlying ASG directly.
    note that this is for internal control.
    for scaling purposes, please use scale() instead.
    """

    logger.info("ACS: {} new_desired_capacity: {}".format(
        self, new_desired_capacity))

    #We only support one agent pool on ACS
    self.instance.agent_pool_profiles[0].count = new_desired_capacity 

    # null out the service principal because otherwise validation complains
    instance.service_principal_profile = None

    client.create_or_update(self.resource_group_name, self.container_service_name, instance)
    
    self.desired_capacity = new_desired_capacity

  