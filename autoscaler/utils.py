import json
import re
import urllib.request
from azure.cli.core.util import get_file_json

SI_suffix = {
    'y': 1e-24,  # yocto
    'z': 1e-21,  # zepto
    'a': 1e-18,  # atto
    'f': 1e-15,  # femto
    'p': 1e-12,  # pico
    'n': 1e-9,  # nano
    'u': 1e-6,  # micro
    'm': 1e-3,  # mili
    'c': 1e-2,  # centi
    'd': 1e-1,  # deci
    'k': 1e3,  # kilo
    'M': 1e6,  # mega
    'G': 1e9,  # giga
    'T': 1e12,  # tera
    'P': 1e15,  # peta
    'E': 1e18,  # exa
    'Z': 1e21,  # zetta
    'Y': 1e24,  # yotta
    # Kube also uses the power of 2 equivalent
    'Ki': 2**10,
    'Mi': 2**20,
    'Gi': 2**30,
    'Ti': 2**40,
    'Pi': 2**50,
    'Ei': 2**60,
}
SI_regex = re.compile(r"(\d+)(%s)?$" % "|".join(SI_suffix.keys()))


def parse_SI(s):
    m = SI_regex.match(s)
    if m is None:
        raise ValueError("Unknown SI quantity: %s" % s)
    num_s, unit = m.groups()
    multiplier = SI_suffix[unit] if unit else 1.  # unitless
    return float(num_s) * multiplier


def parse_resource(resource):
    try:
        return float(resource)
    except ValueError:
        return parse_SI(resource)
def parse_bool_label(value):
    return str(value).lower() in ('1', 'true')

def is_master(node):
    name_parts = node.name.split('-')  
    if len(name_parts) != 4:
        raise ValueError('Kubernetes node name was malformed and cannot be processed.')
    return name_parts[1] == 'master'

def is_agent(node):
    return not is_master(node)

def get_instance_index(node):
    name_parts = node.name.split('-')  
    if len(name_parts) != 4:
        raise ValueError('Kubernetes node name was malformed and cannot be processed.')
    return int(name_parts[3])

def get_pool_name(node):
    name_parts = node.name.split('-')  
    if len(name_parts) != 4:
        raise ValueError('Kubernetes node name was malformed and cannot be processed.')
    return name_parts[1]
  

def order_nodes(node_map):
    """
    takes a map of node and return an ordered list of node.
    The last nodes will be at the end. 
    """
    
    ordered_nodes = []
    
    for node in node_map:   
        if is_master(node): 
            #we want the masters to be at the beginning of the list, as they should never be drained
            #order between the masters doesn't matter
            ordered_nodes.insert(0,node) 
            continue      

        idx=None
        try:
            idx = get_instance_index(node)
        except ValueError:
            raise ValueError('Kubernetes node name was malformed and cannot be processed.')  

        ordered_nodes.insert(idx,node)  

        return ordered_nodes

def get_arm_template(local_file_path, url):
    if local_file_path:
        return get_file_json(local_file_path)
        
    with urllib.request.urlopen(url) as response:
        raw = response.read()
        return json.loads(raw)

def get_arm_parameters(local_file_path, url):
    if local_file_path:
        return get_file_json(local_file_path)
    
    with urllib.request.urlopen(url) as response:
        raw = response.read()
        parameters = json.loads(raw)
        parameters = parameters.get('parameters', parameters)
        return parameters


        