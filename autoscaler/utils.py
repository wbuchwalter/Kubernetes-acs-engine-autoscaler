import json
import re


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


def order_nodes(node_map):
  """
  takes a map of node and return an ordered list of node.
  The last nodes will be at the end. 
  """
  
  ordered_nodes = []
 
  for node in node_map:
    #Format of name: k8s-agent-842efcd6-2
    name_parts = node.name.split('-')
    is_master = False
    if len(name_parts) != 4:
      raise ValueError('Kubernetes node name was malformed and cannot be processed.')

    if name_parts[1] == 'master': 
      #we want the masters to be at the beginning of the list, as they should never be drained
      #order between the masters doesn't matter
      ordered_nodes.insert(0,node) 
      continue      

    idx=-1
    try:
      idx = int(name_parts[3])
    except ValueError:
      raise ValueError('Kubernetes node name was malformed and cannot be processed.')  

    ordered_nodes.insert(idx,node)  

  return ordered_nodes
