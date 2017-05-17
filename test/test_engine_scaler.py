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
from autoscaler.engine_scaler import EngineScaler
from unittest.mock import MagicMock

class TestEngineScaler(unittest.TestCase):
    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'data/node.yaml'), 'r') as f:
            self.dummy_node_ref = yaml.load(f.read())
        with open(os.path.join(dir_path, 'data/busybox.yaml'), 'r') as f:
            self.dummy_pod = yaml.load(f.read())
        self.api = pykube.HTTPClient(pykube.KubeConfig.from_file('~/.kube/config'))
    
    def create_engine_scaler(self, nodes):
        return EngineScaler( 
            resource_group='my-rg',
            nodes=nodes,            
            deployments=None,
            dry_run=False,
            arm_template='fake',
            arm_parameters='fake',
            over_provision=0,
            spare_count=1)

    def create_nodes(self, pool_size, nb_nodes):
        nodes = []
        for pool_idx in range(pool_size):
            for node_idx in range(nb_nodes):
                dummy_node = copy.deepcopy(self.dummy_node_ref)
                node_name = 'k8-agentpool{}-16334397-{}'.format(pool_idx, node_idx)
                dummy_node['metadata']['name'] = node_name
                dummy_node['metadata']['labels']['kubernetes.io/hostname'] = node_name
                node = KubeNode(pykube.Node(self.api, dummy_node))
                nodes.append(node)
        return nodes
    
    def test_get_agent_pools(self):
        nodes = self.create_nodes(2,1)
        engine_scaler = self.create_engine_scaler(nodes)
        
        pools = engine_scaler.get_agent_pools(nodes)
        self.assertEqual(len(pools), 2)    
        self.assertEqual(pools[0].actual_capacity, 1)
        self.assertEqual(pools[1].actual_capacity, 1)

        nodes = self.create_nodes(3,3)
        pools = engine_scaler.get_agent_pools(nodes)
        self.assertEqual(len(pools), 3)    
        self.assertEqual(pools[0].actual_capacity, 3)
        self.assertEqual(pools[1].actual_capacity, 3)
        self.assertEqual(pools[2].actual_capacity, 3)
    
    def test_fulfill_pending(self):
        nodes = self.create_nodes(2,1)
        engine_scaler = self.create_engine_scaler(nodes)
        pod = KubePod(pykube.Pod(self.api, self.dummy_pod))
        engine_scaler.scale_pools = MagicMock()

        engine_scaler.fulfill_pending([pod])
        engine_scaler.scale_pools.assert_called_with({'agentpool0': 2, 'agentpool1': 1}, True)
        

        
        


        