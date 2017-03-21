import pykube
from autoscaler.kube import KubePod, KubeNode, KubeResource, KubePodStatus
import json

pykube.Pod.objects.namespace = None
# we are interested in all pods, incl. system ones
pykube.Pod.objects.namespace = None

# HACK: https://github.com/kelproject/pykube/issues/29#issuecomment-230026930
import backports.ssl_match_hostname
# Monkey-patch match_hostname with backports's match_hostname, allowing for IP addresses
# XXX: the exception that this might raise is
# backports.ssl_match_hostname.CertificateError
pykube.http.requests.packages.urllib3.connection.match_hostname = backports.ssl_match_hostname.match_hostname



def order_nodes(node_map):
  """
  takes a map of node and return an ordered list of node.
  The last nodes will be at the end.
  The master will not be included in the list
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












kubeconfig='/root/.kube/config'
api = pykube.HTTPClient(pykube.KubeConfig.from_file(kubeconfig))
pykube_nodes = pykube.Node.objects(api)
if not pykube_nodes:
  print('Failed to list nodes. Please check kube configuration. Terminating scale loop.')

all_nodes = list(map(KubeNode, pykube_nodes))

# print(json.dumps(all_nodes))
# a = list(all_nodes)[1]
# print(a.name)
order=order_nodes(all_nodes)
for n in order:
  print(n.name)



