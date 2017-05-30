"""
module to handle capacity of resources
"""
import json
from collections import OrderedDict
from autoscaler.config import Config
from autoscaler.kube import KubeResource

# RESOURCE_SPEC should denote the amount of resouces that are available
# to workload pods on a new, clean node, i.e. resouces used by system pods
# have to be accounted for
with open(Config.CAPACITY_DATA, 'r') as f:
    data = json.loads(f.read(), object_pairs_hook=OrderedDict)
    RESOURCE_SPEC = {}
    for instance_type, resource_spec in data.items():
        resource_spec['cpu'] -= Config.CAPACITY_CPU_RESERVE
        resource = KubeResource(**resource_spec)
        RESOURCE_SPEC[instance_type] = resource
DEFAULT_TYPE_SELECTOR_KEY = 'beta.kubernetes.io/instance-type'

def get_capacity_for_instance_type(instance_type):
    return RESOURCE_SPEC[instance_type]

def is_possible(pod, agent_pools):
    """
    returns whether the pod is possible under the maximum allowable capacity
    """
    for pool in agent_pools:
        if (RESOURCE_SPEC[pool.instance_type] - pod.resources).possible:
            return True

    return False

def order_by_cost_asc(agent_pools):
    keys = list(data.keys())
    return sorted(agent_pools, key=lambda x: keys.index(x.instance_type))

