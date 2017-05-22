import unittest
import mock
from unittest.mock import MagicMock
import pykube
import os
import yaml
import json
from copy import deepcopy

from autoscaler.utils import get_arm_template
import autoscaler.template_processing as template_processing
from autoscaler.kube import KubeNode
from autoscaler.scaler import Scaler

class TestTemplateProcessing(unittest.TestCase):
    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'data/node.yaml'), 'r') as f:
            self.dummy_node_ref = yaml.load(f.read())
        with open(os.path.join(dir_path, 'data/busybox.yaml'), 'r') as f:
            self.dummy_pod = yaml.load(f.read())
        self.api = pykube.HTTPClient(pykube.KubeConfig.from_file('~/.kube/config'))
    
    def create_scaler(self, nodes):
        return Scaler( 
            resource_group='my-rg',
            nodes=nodes,            
            deployments=None,
            dry_run=False,
            over_provision=0,
            spare_count=1)

    def create_node(self, pool_name, index):
        dummy_node = deepcopy(self.dummy_node_ref)
        node_name = 'k8-{}-16334397-{}'.format(pool_name, index)
        dummy_node['metadata']['name'] = node_name
        dummy_node['metadata']['labels']['kubernetes.io/hostname'] = node_name
        node = KubeNode(pykube.Node(self.api, dummy_node))
        return node

    def test_unroll_nic(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        template = get_arm_template(os.path.join(dir_path, './data/azuredeploy.original.json'), None)
        expected_template = get_arm_template(os.path.join(dir_path, './data/azuredeploy.expected_nic.json'), None)
        scaler = self.create_scaler([])
        node0 = self.create_node('agentpool1', 0)
        node2 = self.create_node('agentpool1', 2)
        pool = scaler.get_agent_pools([node0])[0]
        new_template = template_processing.unroll_nic(template, pool, 3)
        self.assertDictEqual(new_template, expected_template)
    
    def test_unroll_vm(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        template = get_arm_template(os.path.join(dir_path, './data/azuredeploy.original.json'), None)
        expected_template = get_arm_template(os.path.join(dir_path, './data/azuredeploy.expected_vm.json'), None)
        scaler = self.create_scaler([])
        node0 = self.create_node('agentpool1', 0)
        node2 = self.create_node('agentpool1', 2)
        pool = scaler.get_agent_pools([node0])[0]
        new_template = template_processing.unroll_vm(template, pool, 3)
        self.assertDictEqual(new_template, expected_template)

    def test_get_new_node_indexes(self):
        scaler = self.create_scaler([])
        node0 = self.create_node('agentpool1', 0)
        node1 = self.create_node('agentpool1', 1)
        node2 = self.create_node('agentpool1', 2)
        node3 = self.create_node('agentpool1', 3)
        node4 = self.create_node('agentpool1', 4)
        
        pool = scaler.get_agent_pools([node0])[0]
        new_idxs = template_processing.get_new_nodes_indexes(pool, 2)
        self.assertListEqual(new_idxs, [1])

        pool = scaler.get_agent_pools([node0, node2])[0]
        new_idxs = template_processing.get_new_nodes_indexes(pool, 3)
        self.assertListEqual(new_idxs, [1])

        pool = scaler.get_agent_pools([node0, node1, node2])[0]
        new_idxs = template_processing.get_new_nodes_indexes(pool, 3)
        self.assertListEqual(new_idxs, [])

        pool = scaler.get_agent_pools([node4])[0]
        new_idxs = template_processing.get_new_nodes_indexes(pool, 5)
        self.assertListEqual(new_idxs, [0, 1, 2, 3])

        pool = scaler.get_agent_pools([node2])[0]
        new_idxs = template_processing.get_new_nodes_indexes(pool, 5)
        self.assertListEqual(new_idxs, [0, 1, 3, 4])