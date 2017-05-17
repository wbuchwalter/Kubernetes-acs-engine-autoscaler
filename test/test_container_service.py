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
from autoscaler.container_service import ContainerService
from unittest.mock import MagicMock

class TestContainerService(unittest.TestCase):
    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'data/node.yaml'), 'r') as f:
            self.dummy_node_ref = yaml.load(f.read())
        with open(os.path.join(dir_path, 'data/busybox.yaml'), 'r') as f:
            self.dummy_pod = yaml.load(f.read())
        self.api = pykube.HTTPClient(pykube.KubeConfig.from_file('~/.kube/config'))
    
    def create_container_service(self, nodes):
        return ContainerService( 
            resource_group='my-rg',
            nodes=nodes,            
            deployments=None,
            dry_run=False,
            container_service_name=None,
            arm_template='fake',
            arm_parameters='fake',
            over_provision=0)

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
        container_service = self.create_container_service(nodes)
        
        pools = container_service.get_agent_pools(nodes)
        self.assertEqual(len(pools), 2)    
        self.assertEqual(pools[0].actual_capacity, 1)
        self.assertEqual(pools[1].actual_capacity, 1)

        nodes = self.create_nodes(3,3)
        pools = container_service.get_agent_pools(nodes)
        self.assertEqual(len(pools), 3)    
        self.assertEqual(pools[0].actual_capacity, 3)
        self.assertEqual(pools[1].actual_capacity, 3)
        self.assertEqual(pools[2].actual_capacity, 3)
    
    def test_fulfill_pending(self):
        nodes = self.create_nodes(2,1)
        container_service = self.create_container_service(nodes)
        pod = KubePod(pykube.Pod(self.api, self.dummy_pod))
        container_service.scale_pools = MagicMock()

        container_service.fulfill_pending([pod])
        container_service.scale_pools.assert_called_with({'agentpool0': 2, 'agentpool1': 1}, True)
        

        
        


        