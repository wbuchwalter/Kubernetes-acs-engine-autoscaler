import os.path
import unittest

import boto3
import pykube
import mock
import moto
import yaml

from autoscaler.cluster import Cluster
from autoscaler.kube import KubePod, KubeNode
import autoscaler.utils as utils


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

        # this isn't actually used here
        # only needed to create the KubePod object...
        self.api = pykube.HTTPClient(pykube.KubeConfig.from_file('~/.kube/config'))

        # start creating our mock ec2 environment
        self.mocks = [moto.mock_ec2(), moto.mock_autoscaling()]
        for moto_mock in self.mocks:
            moto_mock.start()

        client = boto3.client('autoscaling')
        self.asg_client = client

        client.create_launch_configuration(
            LaunchConfigurationName='dummy-lc',
            ImageId='ami-deadbeef',
            KeyName='dummy-key',
            SecurityGroups=[
                'sg-cafebeef',
            ],
            InstanceType='t2.medium'
        )

        client.create_auto_scaling_group(
            AutoScalingGroupName='dummy-asg',
            LaunchConfigurationName='dummy-lc',
            MinSize=0,
            MaxSize=10,
            VPCZoneIdentifier='subnet-beefbeef',
            Tags=[
                {
                    'Key': 'KubernetesCluster',
                    'Value': 'dummy-cluster',
                    'PropagateAtLaunch': True
                },
                {
                    'Key': 'KubernetesRole',
                    'Value': 'worker',
                    'PropagateAtLaunch': True
                }
            ]
        )

        # finally our cluster
        self.cluster = Cluster(
            aws_access_key='',
            aws_secret_key='',
            regions=['us-west-2', 'us-east-1', 'us-west-1'],
            kubeconfig='~/.kube/config',
            idle_threshold=60,
            instance_init_time=60,
            type_idle_threshold=60,
            cluster_name='dummy-cluster',
            slack_hook='',
            dry_run=False
        )

    def tearDown(self):
        for moto_mock in self.mocks:
            moto_mock.stop()

    def _spin_up_node(self):
        # spin up dummy ec2 node
        self.asg_client.set_desired_capacity(AutoScalingGroupName='dummy-asg',
                                             DesiredCapacity=1)
        response = self.asg_client.describe_auto_scaling_groups()
        instance_id = response['AutoScalingGroups'][0]['Instances'][0]['InstanceId']

        self.dummy_node['metadata']['labels']['aws/id'] = instance_id
        node = KubeNode(pykube.Node(self.api, self.dummy_node))
        return node

    def test_scale_up_selector(self):
        self.dummy_pod['spec']['nodeSelector'] = {
            'aws/type': 'm4.large'
        }
        pod = KubePod(pykube.Pod(self.api, self.dummy_pod))
        selectors_hash = utils.selectors_to_hash(pod.selectors)
        asgs = self.cluster.autoscaling_groups.get_all_groups([])
        self.cluster.fulfill_pending(asgs, selectors_hash, [pod])

        response = self.asg_client.describe_auto_scaling_groups()
        assert len(response['AutoScalingGroups']) == 1
        assert response['AutoScalingGroups'][0]['DesiredCapacity'] == 0

    def test_scale_up(self):
        pod = KubePod(pykube.Pod(self.api, self.dummy_pod))
        selectors_hash = utils.selectors_to_hash(pod.selectors)
        asgs = self.cluster.autoscaling_groups.get_all_groups([])
        self.cluster.fulfill_pending(asgs, selectors_hash, [pod])

        response = self.asg_client.describe_auto_scaling_groups()
        assert len(response['AutoScalingGroups']) == 1
        assert response['AutoScalingGroups'][0]['DesiredCapacity'] > 0

    def test_scale_down(self):
        """
        kube node with daemonset and no pod --> cordon
        """
        node = self._spin_up_node()
        node.cordon = mock.Mock(return_value="mocked stuff")
        node.drain = mock.Mock(return_value="mocked stuff")
        all_nodes = [node]
        managed_nodes = [n for n in all_nodes if node.is_managed()]
        running_insts_map = self.cluster.get_running_instances_map(managed_nodes)
        pods_to_schedule = {}
        asgs = self.cluster.autoscaling_groups.get_all_groups(all_nodes)

        ds_pod = KubePod(pykube.Pod(self.api, self.dummy_ds_pod))
        running_or_pending_assigned_pods = [ds_pod]

        self.cluster.idle_threshold = -1
        self.cluster.type_idle_threshold = -1
        self.cluster.maintain(
            managed_nodes, running_insts_map,
            pods_to_schedule, running_or_pending_assigned_pods, asgs)

        response = self.asg_client.describe_auto_scaling_groups()
        assert len(response['AutoScalingGroups']) == 1
        assert response['AutoScalingGroups'][0]['DesiredCapacity'] == 1
        node.cordon.assert_called_once_with()

    def test_scale_down_grace_period(self):
        """
        kube node with daemonset and no pod + grace period --> noop
        """
        node = self._spin_up_node()
        node.cordon = mock.Mock(return_value="mocked stuff")
        all_nodes = [node]
        managed_nodes = [n for n in all_nodes if node.is_managed()]
        running_insts_map = self.cluster.get_running_instances_map(managed_nodes)
        pods_to_schedule = {}
        asgs = self.cluster.autoscaling_groups.get_all_groups(all_nodes)

        # kube node with daemonset and no pod --> cordon
        ds_pod = KubePod(pykube.Pod(self.api, self.dummy_ds_pod))
        running_or_pending_assigned_pods = [ds_pod]

        self.cluster.maintain(
            managed_nodes, running_insts_map,
            pods_to_schedule, running_or_pending_assigned_pods, asgs)

        response = self.asg_client.describe_auto_scaling_groups()
        assert len(response['AutoScalingGroups']) == 1
        assert response['AutoScalingGroups'][0]['DesiredCapacity'] == 1
        node.cordon.assert_not_called()

    def test_scale_down_busy(self):
        """
        kube node with daemonset and pod/rc-pod --> noop
        """
        node = self._spin_up_node()
        node.cordon = mock.Mock(return_value="mocked stuff")
        all_nodes = [node]
        managed_nodes = [n for n in all_nodes if node.is_managed()]
        running_insts_map = self.cluster.get_running_instances_map(managed_nodes)
        pods_to_schedule = {}
        asgs = self.cluster.autoscaling_groups.get_all_groups(all_nodes)

        # kube node with daemonset and no pod --> cordon
        ds_pod = KubePod(pykube.Pod(self.api, self.dummy_ds_pod))
        pod = KubePod(pykube.Pod(self.api, self.dummy_pod))
        running_or_pending_assigned_pods = [ds_pod, pod]

        self.cluster.maintain(
            managed_nodes, running_insts_map,
            pods_to_schedule, running_or_pending_assigned_pods, asgs)

        response = self.asg_client.describe_auto_scaling_groups()
        assert len(response['AutoScalingGroups']) == 1
        assert response['AutoScalingGroups'][0]['DesiredCapacity'] == 1
        node.cordon.assert_not_called()

        # kube node with daemonset and rc pod --> noop
        ds_pod = KubePod(pykube.Pod(self.api, self.dummy_ds_pod))
        rc_pod = KubePod(pykube.Pod(self.api, self.dummy_rc_pod))
        running_or_pending_assigned_pods = [ds_pod, rc_pod]

        self.cluster.maintain(
            managed_nodes, running_insts_map,
            pods_to_schedule, running_or_pending_assigned_pods, asgs)

        response = self.asg_client.describe_auto_scaling_groups()
        assert len(response['AutoScalingGroups']) == 1
        assert response['AutoScalingGroups'][0]['DesiredCapacity'] == 1
        node.cordon.assert_not_called()
