import unittest
import mock
from autoscaler.cluster import Cluster
import os.path
import yaml
import collections
import json
import copy
from datetime import datetime, timedelta
import pykube
from autoscaler.kube import KubePod, KubeNode, KubeResource

class TestCluster(unittest.TestCase):
    def setUp(self):
        # load dummy kube specs
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'data/busybox.yaml'), 'r') as f:
            self.dummy_pod = yaml.load(f.read())
        with open(os.path.join(dir_path, 'data/ds-pod.yaml'), 'r') as f:
            self.dummy_ds_pod = yaml.load(f.read())
        with open(os.path.join(dir_path, 'data/rc-pod.yaml'), 'r') as f:
            self.dummy_rc_pod = yaml.load(f.read())
        with open(os.path.join(dir_path, 'data/node.yaml'), 'r') as f:
            self.dummy_node = yaml.load(f.read())
            for condition in self.dummy_node['status']['conditions']:
                if condition['type'] == 'Ready' and condition['status'] == 'True':
                    condition['lastHeartbeatTime'] = datetime.now(condition['lastHeartbeatTime'].tzinfo)
            # Convert timestamps to strings to match PyKube
            for condition in self.dummy_node['status']['conditions']:
                condition['lastHeartbeatTime'] = datetime.isoformat(condition['lastHeartbeatTime'])
                condition['lastTransitionTime'] = datetime.isoformat(condition['lastTransitionTime'])
        
        # this isn't actually used here
        # only needed to create the KubePod object...
        self.api = pykube.HTTPClient(pykube.KubeConfig.from_file('~/.kube/config'))
        
        self.cluster = Cluster(
            kubeconfig='~/.kube/config',
            idle_threshold=60,
            spare_agents=1,
            instance_init_time=60,
            resource_group='my-rg',
            notifier=None,
            service_principal_app_id='dummy',
            service_principal_secret='dummy',
            service_principal_tenant_id='dummy',
            kubeconfig_private_key='dummy',
            client_private_key='dummy',
            ignore_pools=''
        )

    def test_get_pending_pods(self):
        dummy_node = copy.deepcopy(self.dummy_node)
        dummy_node['metadata']['name'] = 'k8s-agentpool1-16334397-0'
        node = KubeNode(pykube.Node(self.api, dummy_node))
        pod = KubePod(pykube.Pod(self.api, self.dummy_pod))

        act = self.cluster.get_pending_pods([pod], [node])
        self.assertEqual(len(act), 0)

        node = KubeNode(pykube.Node(self.api, dummy_node))
        pod2 = KubePod(pykube.Pod(self.api, self.dummy_pod))
        pod3 = KubePod(pykube.Pod(self.api, self.dummy_pod))

        act = self.cluster.get_pending_pods([pod, pod2, pod3], [node])
        #only one should fit
        self.assertEqual(len(act), 2)  
    
