import unittest
import mock
import os.path
import yaml
import collections
import json
import copy
from datetime import datetime, timedelta
import pykube
from autoscaler.kube import KubePod, KubeNode, KubeResource
from autoscaler.scaler import Scaler
from unittest.mock import MagicMock
from autoscaler.utils import get_arm_template

class TestScaler(unittest.TestCase):
    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'data/node.yaml'), 'r') as f:
            self.dummy_node_ref = yaml.load(f.read())
        with open(os.path.join(dir_path, 'data/busybox.yaml'), 'r') as f:
            self.dummy_pod = yaml.load(f.read())
        self.api = pykube.HTTPClient(pykube.KubeConfig.from_file('~/.kube/config'))
    
    def create_scaler(self, nodes):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        template = get_arm_template(os.path.join(dir_path, './data/azuredeploy.cluster.json'), None)
        parameters = get_arm_template(os.path.join(dir_path, './data/azuredeploy.cluster.parameters.json'), None)
        return EngineScaler( 
            resource_group='my-rg',
            nodes=nodes,            
            deployments=None,
            dry_run=False,
            over_provision=0,
            spare_count=1,
            arm_parameters=parameters,
            arm_template=template,
            ignore_pools=''
            )

    def create_nodes(self, nb_pool, nb_nodes_per_pool):
        nodes = []
        for pool_idx in range(nb_pool):
            for node_idx in range(nb_nodes_per_pool):
                dummy_node = copy.deepcopy(self.dummy_node_ref)
                node_name = 'k8-agentpool{}-16334397-{}'.format(pool_idx, node_idx)
                dummy_node['metadata']['name'] = node_name
                dummy_node['metadata']['labels']['kubernetes.io/hostname'] = node_name
                node = KubeNode(pykube.Node(self.api, dummy_node))
                nodes.append(node)
        return nodes
    
    def test_get_agent_pools(self):
        nodes = self.create_nodes(2,1)
        scaler = self.create_scaler(nodes)
        pools = scaler.agent_pools
        
        self.assertEqual(len(pools), 2)    
        self.assertEqual(pools[0].actual_capacity, 1)
        self.assertEqual(pools[1].actual_capacity, 1)

        nodes = self.create_nodes(3,3)
        pools = scaler.get_agent_pools(nodes)
        self.assertEqual(len(pools), 3)    
        self.assertEqual(pools[0].actual_capacity, 3)
        self.assertEqual(pools[1].actual_capacity, 3)
        self.assertEqual(pools[2].actual_capacity, 3)
    
    def test_fulfill_pending(self):
        nodes = self.create_nodes(2,1)
        scaler = self.create_scaler(nodes)
        scaler.scale_pools = MagicMock()

        #Should add one node to pool 1 (2, 1)
        pod = KubePod(pykube.Pod(self.api, self.dummy_pod))
        scaler.fulfill_pending([pod])
        scaler.scale_pools.assert_called_with({'agentpool0': 2, 'agentpool1': 1})
        
        #The two pods should fit in the same new node
        dummy_pod_2 = copy.deepcopy(self.dummy_pod)
        dummy_pod_2['spec']['containers'][0]['resources']['requests']['cpu'] = '400m'
        dummy_pod_2['spec']['containers'][0]['resources']['limits']['cpu'] = '400m'
        dummy_pod_2['metadata']['uid'] = 'fake'
        pod_2 = KubePod(pykube.Pod(self.api, dummy_pod_2))
        scaler.fulfill_pending([pod, pod_2])
        scaler.scale_pools.assert_called_with({'agentpool0': 2, 'agentpool1': 1})

        #pod_2 shouldn't fit anymore, and so it should add 2 new VMs
        dummy_pod_2['spec']['containers'][0]['resources']['requests']['cpu'] = '600m'
        dummy_pod_2['spec']['containers'][0]['resources']['limits']['cpu'] = '600m'
        pod_2 = KubePod(pykube.Pod(self.api, dummy_pod_2))
        scaler.fulfill_pending([pod, pod_2])
        scaler.scale_pools.assert_called_with({'agentpool0': 3, 'agentpool1': 1})
    



        
        


        