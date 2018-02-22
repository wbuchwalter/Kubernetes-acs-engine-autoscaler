import collections
import datetime
import logging
import math
import time
import sys
import pykube
import os

from autoscaler.azure_api import login, download_parameters, download_template
from autoscaler.engine_scaler import EngineScaler
import autoscaler.capacity as capacity
from autoscaler.kube import KubePod, KubeNode, KubeResource, KubePodStatus
import autoscaler.utils as utils
from autoscaler.deployments import Deployments
from autoscaler.template_processing import delete_master_vm_extension

from msrestazure.azure_exceptions import CloudError

# we are interested in all pods, incl. system ones
pykube.Pod.objects.namespace = None

# HACK: https://github.com/kelproject/pykube/issues/29#issuecomment-230026930
import backports.ssl_match_hostname
# Monkey-patch match_hostname with backports's match_hostname, allowing for IP addresses
# XXX: the exception that this might raise is
# backports.ssl_match_hostname.CertificateError
pykube.http.requests.packages.urllib3.connection.match_hostname = backports.ssl_match_hostname.match_hostname

logger = logging.getLogger(__name__)

class Cluster(object):
    def __init__(self, kubeconfig, idle_threshold, spare_agents, 
                 service_principal_app_id, service_principal_secret, service_principal_tenant_id, subscription_id,
                 client_private_key, ca_private_key,
                 instance_init_time, resource_group, notifier, ignore_pools,
                 acs_deployment='azuredeploy',
                 scale_up=True, maintainance=True,
                 over_provision=5, dry_run=False):

        # config
        self.kubeconfig = kubeconfig
        self.service_principal_app_id = service_principal_app_id
        self.service_principal_secret = service_principal_secret
        self.service_principal_tenant_id = service_principal_tenant_id
        self.subscription_id = subscription_id
        self.client_private_key = client_private_key
        self.ca_private_key = ca_private_key
        self._drained = {}
        self.resource_group = resource_group
        self.acs_deployment = acs_deployment
        self.agent_pools = {}
        self.pools_instance_type = {}
        self.instance_init_time = instance_init_time
        self.spare_agents = spare_agents
        self.idle_threshold = idle_threshold
        self.over_provision = over_provision
        self.scale_up = scale_up
        self.maintainance = maintainance
        self.notifier = notifier
        self.dry_run = dry_run
        self.deployments = Deployments()
        self.ignore_pools = ignore_pools

    def login(self):
        subscriptions = login(
            self.service_principal_app_id,
            self.service_principal_secret,
            self.service_principal_tenant_id,
            self.subscription_id)

        self.arm_template = download_template(self.resource_group, self.acs_deployment)
        self.arm_parameters = download_parameters(self.resource_group, self.acs_deployment)
        #downloaded parameters do not include SecureStrings parameters, so we need to fill them manually
        self.fill_parameters_secure_strings()
        
        if 'firstConsecutiveStaticIP' in self.arm_parameters:
            #firstConsecutiveStaticIP parameter is used as the private IP for the master
            os.environ["PYKUBE_KUBERNETES_SERVICE_HOST"] = self.arm_parameters['firstConsecutiveStaticIP']['value']

        if self.kubeconfig:
            # for using locally
            logger.debug('Using kubeconfig %s', self.kubeconfig)
            self.api = pykube.HTTPClient(
                pykube.KubeConfig.from_file(self.kubeconfig))
        else:
            # for using on kube
            logger.debug('Using kube service account')
            self.api = pykube.HTTPClient(
                pykube.KubeConfig.from_service_account())
    
    def fill_parameters_secure_strings(self):
        self.arm_parameters['clientPrivateKey'] = {'value': self.client_private_key}
        self.arm_parameters['servicePrincipalClientId'] = {'value': self.service_principal_app_id}
        self.arm_parameters['servicePrincipalClientSecret'] = {'value': self.service_principal_secret}
       
        dummy_key = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tDQpNSUlNSnpDQ0NnK2dBd0lCQWdJUkFMOThkQ2lFc1I0UC9vT0QzdlV5SDAwd0RRWUpLb1pJaHZjTkFRRUxCUUF3DQpEVEVMTUFrR0ExVUVBeE1DWTJFd0hoY05NVGd3TWpJeU1ESXlNekE0V2hjTk1qQXdNakl5TURJeU16QTRXakFVDQpNUkl3RUFZRFZRUURFd2xoY0dselpYSjJaWEl3Z2dJaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQ0R3QXdnZ0lLDQpBb0lDQVFDbUJ6dVhqbFJvUGwycGlPZnlLazVxSUNFRG43bm1XVUlzdjNKcDAyZk5OTFQzK2Y2dVZIQmxqS3VhDQpMTG9XeW1TNTMrREgyWnFtc25sQkM1Y3FHdVV2dnJwT0hpZEZ0L3JwNkpjYWw2MEVkS0pPVWRMMW93NTgzMlc4DQozY1d1M3hzNHZJN2VWRGVZNWZaTnFkcmd0MW4yTGlPdExJYXdoZXdKNlcxV2tzTlBId2IrRC83TlpPb1dtZ3BnDQo0V1RsT3JJZWJNV3FlWm1kb3N4M3ZEWmIvQ0grQjgxbFFJbXlncFFLWXk3THVzejJPU2tjMUVUcWxka2FFeEdxDQpwOVhIK0tGNU1YYlp5YS9TVk5MdlN2QjFDWkY4OERrSHZMaUROWFdHaTZBaUxndU1RSzFqSUZCb0l2QllXTHZSDQpzR1I1bnhSR3RJWG5YYW9RV2dtMjQ4Sm4xd1h2UytNU3A3cEtXSWFHRzlTcmM0R1Q0amRqMmhRZy8zQXEreDFpDQpQanoxS2w1cXdZSmQrRlhsT2RPOSsxbldpdklFL2VlOHZTUHc0S2hDbnB1a1dyODlSKy9wQStZZy9PeUF4bWFRDQpDTnEzQWtOSW9aMGNrTytMOExpNVVCNFFLN1h3KzZyMFVmZ0p4clB3RklEVDc0L1NPeXUzdFVqNjc3OG5xdk9rDQpranJFYi9RSEVoV1BVUVV1emVJdDE0bVdRSDJUS0ZkMnJoSGZVMFA2ZG1XVmtJSmt1OHJSMWpHOTZSWURLRzlrDQoxamovazVCcG5ORWZMVEF0K1A0NURxb1lOL1dpNWNkNys0aVdVMHhSTWE0bWpxLzU5Zml3eWl4SithbEVHTzhxDQpsNkg3NXJYTXlXR1ZzMUNCM0E5Z1haOXdHSVpJVjlyREl6aVZxMERHcnhaWUo4YjFyd0lEQVFBQm80SUhlVENDDQpCM1V3RGdZRFZSMFBBUUgvQkFRREFnV2dNQk1HQTFVZEpRUU1NQW9HQ0NzR0FRVUZCd01CTUF3R0ExVWRFd0VCDQovd1FDTUFBd2dnYytCZ05WSFJFRWdnYzFNSUlITVlJb2QybGlkV05vTWk1aGRYTjBjbUZzYVdGbFlYTjBMbU5zDQpiM1ZrWVhCd0xtRjZkWEpsTG1OdmJZSXRkMmxpZFdOb01pNWhkWE4wY21Gc2FXRnpiM1YwYUdWaGMzUXVZMnh2DQpkV1JoY0hBdVlYcDFjbVV1WTI5dGdpWjNhV0oxWTJneUxtSnlZWHBwYkhOdmRYUm9MbU5zYjNWa1lYQndMbUY2DQpkWEpsTG1OdmJZSW9kMmxpZFdOb01pNWpZVzVoWkdGalpXNTBjbUZzTG1Oc2IzVmtZWEJ3TG1GNmRYSmxMbU52DQpiWUlsZDJsaWRXTm9NaTVqWVc1aFpHRmxZWE4wTG1Oc2IzVmtZWEJ3TG1GNmRYSmxMbU52YllJbmQybGlkV05vDQpNaTVqWlc1MGNtRnNhVzVrYVdFdVkyeHZkV1JoY0hBdVlYcDFjbVV1WTI5dGdpUjNhV0oxWTJneUxtTmxiblJ5DQpZV3gxY3k1amJHOTFaR0Z3Y0M1aGVuVnlaUzVqYjIyQ0tIZHBZblZqYURJdVkyVnVkSEpoYkhWelpYVmhjQzVqDQpiRzkxWkdGd2NDNWhlblZ5WlM1amIyMkNLM2RwWW5WamFESXVZMmhwYm1GbFlYTjBMbU5zYjNWa1lYQndMbU5vDQphVzVoWTJ4dmRXUmhjR2t1WTI2Q0xIZHBZblZqYURJdVkyaHBibUZ1YjNKMGFDNWpiRzkxWkdGd2NDNWphR2x1DQpZV05zYjNWa1lYQnBMbU51Z2lOM2FXSjFZMmd5TG1WaGMzUmhjMmxoTG1Oc2IzVmtZWEJ3TG1GNmRYSmxMbU52DQpiWUloZDJsaWRXTm9NaTVsWVhOMGRYTXVZMnh2ZFdSaGNIQXVZWHAxY21VdVkyOXRnaUozYVdKMVkyZ3lMbVZoDQpjM1IxY3pJdVkyeHZkV1JoY0hBdVlYcDFjbVV1WTI5dGdpWjNhV0oxWTJneUxtVmhjM1IxY3pKbGRXRndMbU5zDQpiM1ZrWVhCd0xtRjZkWEpsTG1OdmJZSWtkMmxpZFdOb01pNXFZWEJoYm1WaGMzUXVZMnh2ZFdSaGNIQXVZWHAxDQpjbVV1WTI5dGdpUjNhV0oxWTJneUxtcGhjR0Z1ZDJWemRDNWpiRzkxWkdGd2NDNWhlblZ5WlM1amIyMkNKM2RwDQpZblZqYURJdWEyOXlaV0ZqWlc1MGNtRnNMbU5zYjNWa1lYQndMbUY2ZFhKbExtTnZiWUlsZDJsaWRXTm9NaTVyDQpiM0psWVhOdmRYUm9MbU5zYjNWa1lYQndMbUY2ZFhKbExtTnZiWUlwZDJsaWRXTm9NaTV1YjNKMGFHTmxiblJ5DQpZV3gxY3k1amJHOTFaR0Z3Y0M1aGVuVnlaUzVqYjIyQ0puZHBZblZqYURJdWJtOXlkR2hsZFhKdmNHVXVZMnh2DQpkV1JoY0hBdVlYcDFjbVV1WTI5dGdpbDNhV0oxWTJneUxuTnZkWFJvWTJWdWRISmhiSFZ6TG1Oc2IzVmtZWEJ3DQpMbUY2ZFhKbExtTnZiWUlvZDJsaWRXTm9NaTV6YjNWMGFHVmhjM1JoYzJsaExtTnNiM1ZrWVhCd0xtRjZkWEpsDQpMbU52YllJbGQybGlkV05vTWk1emIzVjBhR2x1WkdsaExtTnNiM1ZrWVhCd0xtRjZkWEpsTG1OdmJZSWlkMmxpDQpkV05vTWk1MWEzTnZkWFJvTG1Oc2IzVmtZWEJ3TG1GNmRYSmxMbU52YllJaGQybGlkV05vTWk1MWEzZGxjM1F1DQpZMnh2ZFdSaGNIQXVZWHAxY21VdVkyOXRnaWgzYVdKMVkyZ3lMbmRsYzNSalpXNTBjbUZzZFhNdVkyeHZkV1JoDQpjSEF1WVhwMWNtVXVZMjl0Z2lWM2FXSjFZMmd5TG5kbGMzUmxkWEp2Y0dVdVkyeHZkV1JoY0hBdVlYcDFjbVV1DQpZMjl0Z2lSM2FXSjFZMmd5TG5kbGMzUnBibVJwWVM1amJHOTFaR0Z3Y0M1aGVuVnlaUzVqYjIyQ0lYZHBZblZqDQphREl1ZDJWemRIVnpMbU5zYjNWa1lYQndMbUY2ZFhKbExtTnZiWUlpZDJsaWRXTm9NaTUzWlhOMGRYTXlMbU5zDQpiM1ZrWVhCd0xtRjZkWEpsTG1OdmJZSXJkMmxpZFdOb01pNWphR2x1WVdWaGMzUXVZMnh2ZFdSaGNIQXVZMmhwDQpibUZqYkc5MVpHRndhUzVqYm9Jc2QybGlkV05vTWk1amFHbHVZVzV2Y25Sb0xtTnNiM1ZrWVhCd0xtTm9hVzVoDQpZMnh2ZFdSaGNHa3VZMjZDTVhkcFluVmphREl1WjJWeWJXRnVlV05sYm5SeVlXd3VZMnh2ZFdSaGNIQXViV2xqDQpjbTl6YjJaMFlYcDFjbVV1WkdXQ00zZHBZblZqYURJdVoyVnliV0Z1ZVc1dmNuUm9aV0Z6ZEM1amJHOTFaR0Z3DQpjQzV0YVdOeWIzTnZablJoZW5WeVpTNWtaWUl3ZDJsaWRXTm9NaTUxYzJkdmRuWnBjbWRwYm1saExtTnNiM1ZrDQpZWEJ3TG5WeloyOTJZMnh2ZFdSaGNHa3VibVYwZ2l4M2FXSjFZMmd5TG5WeloyOTJhVzkzWVM1amJHOTFaR0Z3DQpjQzUxYzJkdmRtTnNiM1ZrWVhCcExtNWxkSUl2ZDJsaWRXTm9NaTUxYzJkdmRtRnlhWHB2Ym1FdVkyeHZkV1JoDQpjSEF1ZFhObmIzWmpiRzkxWkdGd2FTNXVaWFNDTFhkcFluVmphREl1ZFhObmIzWjBaWGhoY3k1amJHOTFaR0Z3DQpjQzUxYzJkdmRtTnNiM1ZrWVhCcExtNWxkSUlvZDJsaWRXTm9NaTVtY21GdVkyVmpaVzUwY21Gc0xtTnNiM1ZrDQpZWEJ3TG1GNmRYSmxMbU52YllJS2EzVmlaWEp1WlhSbGM0SVNhM1ZpWlhKdVpYUmxjeTVrWldaaGRXeDBnaFpyDQpkV0psY201bGRHVnpMbVJsWm1GMWJIUXVjM1pqZ2lScmRXSmxjbTVsZEdWekxtUmxabUYxYkhRdWMzWmpMbU5zDQpkWE4wWlhJdWJHOWpZV3lDRm10MVltVnlibVYwWlhNdWEzVmlaUzF6ZVhOMFpXMkNHbXQxWW1WeWJtVjBaWE11DQphM1ZpWlMxemVYTjBaVzB1YzNaamdpaHJkV0psY201bGRHVnpMbXQxWW1VdGMzbHpkR1Z0TG5OMll5NWpiSFZ6DQpkR1Z5TG14dlkyRnNod1FLOEFBRWh3UUs4QUFPaHdRSzhBQUZod1FLOEFBR2h3UUtBQUFCTUEwR0NTcUdTSWIzDQpEUUVCQ3dVQUE0SUNBUUNIVWtJZ011bU4wNElCaXZNcUQxRGxKbGhKSGU4MmFRaWFoZ1VTT1lyek0xUitmYmxZDQpoOTNqeHYxOUNHRjNHQXd0YmxPVUVHWkZyTWJEbWlSZ2lCeTNqaXBJQlpXWlJ2Tm9tS0Z0UEw1WnZoNFhMRWY2DQpOcXRKZmt2TU45czU5UTBROGt0dE5FcGI1YXQvSzNiVEJESkJCb2FiMExMSFN6OFBEUWVyb3VGdjFHaFVKcmNuDQpkOVdqWFp6NDFFaVBsMm5PUVNNT3NZbC9QQ3FKa2tuVk5qeisxUE1zcjU2ZWI3d3V0ZnlacHRKSnY5aE04NnRyDQpPNXc0aEtaajJmNWh1SjFKVUp3NXVoMVlOS01YVVR2UGRMODNBUERxSUNvckFobGFxb0QwMVNmeFZrQnI5czF6DQpsMGd6WUxqdGZLWHdhdEVISlcvK1RGWlA2SVJDUjFDaXhxbkhZNS9LN3FmbThFVUJBV3lzdTBxOUFteWFoSmFhDQpWSzIyQmFPeStmMFdLT3N4WGxLNVcvZEViMS9iVkN5R2Q5S2hDRCtUbWw4OS9LMzJ1MkhLTFJBdnlIVGtOMDRMDQp1cTNJdHRFRW1QaGlLOUI3MWQ5TW5TQVUzYzV4YVJ2NmNFQkVOUHp6VUhRa3Z6OWlsNzlEbkhpb3VtMW1FSkh6DQpKZ3NjNkl6bFE1c3pmMVd6bXNtQmpSeTgwRktacUNQNWlpb2pIKys1S21HSUNNVnJoTVFoYzVQRVlmSUp4ckhqDQpiNUNFb3lYQW9jWlFuSnFZdG9XaSt6NU4yRFpvMHhLbVN6ck9IS3dNUGlOUkxQbjNBakhQZnAxQ0RyRXpMMzZvDQpuNFUxc3lhbVd0VlQydDY1UDAyOXBGbi8zUlZ1V3JqUlRMYThFSXo5VzREK2lZQ3MxZTNDYThWb3FnPT0NCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0NCg=="
        self.arm_parameters['caPrivateKey'] = {'value': dummy_key}
        self.arm_parameters['kubeConfigPrivateKey'] = {'value': dummy_key}
        self.arm_parameters['apiServerPrivateKey'] = {'value': dummy_key}
        self.arm_parameters['etcdClientPrivateKey'] = {'value': dummy_key}
        self.arm_parameters['etcdServerPrivateKey'] = {'value': dummy_key}

        for i in range(5):
            key = "etcdPeerPrivateKey{}".format(i)
            if key in self.arm_parameters:
                self.arm_parameters[key] = {'value': dummy_key}


        self.arm_template = delete_master_vm_extension(self.arm_template)

    def loop(self, debug):
        """
        runs one loop of scaling to current needs.
        returns True if successfully scaled.
        """
        logger.info("++++ Running Scaling Loop ++++++")

        if debug:
            # In debug mode, we don't want to catch error. Let the app crash
            # explicitly
            logger.info('Debug mode is on')
            return self.loop_logic()
        else:
            try:
                return self.loop_logic()
            except Exception as e:
                logger.error("Unexpected error: {}, {}".format(sys.exc_info()[0], e))
                return False
    
    def create_kube_node(self, node):
        kube_node = KubeNode(node)
        kube_node.capacity = capacity.get_capacity_for_instance_type(kube_node.instance_type)
        return kube_node

    def loop_logic(self):
        pykube_nodes = pykube.Node.objects(self.api)
        if not pykube_nodes:
            logger.warn(
                'Failed to list nodes. Please check kube configuration. Terminating scale loop.')
            return False

        all_nodes = list(filter(utils.is_agent, map(self.create_kube_node, pykube_nodes)))

        scaler = EngineScaler(
            resource_group=self.resource_group,
            nodes=all_nodes,
            deployments=self.deployments,
            arm_template=self.arm_template,
            arm_parameters=self.arm_parameters,
            dry_run=self.dry_run,
            ignore_pools=self.ignore_pools,
            over_provision=self.over_provision,
            spare_count=self.spare_agents,
            idle_threshold=self.idle_threshold,
            notifier=self.notifier)

        pods = list(map(KubePod, pykube.Pod.objects(self.api)))

        running_or_pending_assigned_pods = [
            p for p in pods if (p.status == KubePodStatus.RUNNING or p.status == KubePodStatus.CONTAINER_CREATING) or (
                p.status == KubePodStatus.PENDING and p.node_name
            )
        ]

        for node in all_nodes:
            for pod in running_or_pending_assigned_pods:
                if pod.node_name == node.name:
                    node.count_pod(pod)
        pods_to_schedule = self.get_pods_to_schedule(pods, scaler.agent_pools)
        logger.info("Pods to schedule: {}".format(len(pods_to_schedule)))

        if self.scale_up:
            logger.info("++++ Scaling Up Begins ++++++")
            self.scale(pods_to_schedule, all_nodes, scaler)
            logger.info("++++ Scaling Up Ends ++++++")
        if self.maintainance:
            logger.info("++++ Maintenance Begins ++++++")
            self.maintain(pods_to_schedule,
                          running_or_pending_assigned_pods, scaler)
            logger.info("++++ Maintenance Ends ++++++")

        return True

    def get_pending_pods(self, pods, nodes):
        pending_pods = []
        # for each pending & unassigned job, try to fit them on current machines or count requested
        #   resources towards future machines
        for pod in pods:
            fitting = None
            for node in nodes:
                if node.can_fit(pod.resources):
                    fitting = node
                    break
            if fitting is None:
                pending_pods.append(pod)
            else:
                fitting.count_pod(pod)
                logger.info("{pod} fits on {node}".format(pod=pod,
                                                          node=fitting))
        logger.info("Pending pods: {}".format(len(pending_pods)))
        for pod in pending_pods:
            logger.debug(pod.name)

        return pending_pods

    def scale(self, pods_to_schedule, nodes, scaler):
        """
        scale up logic
        """
        logger.info("Nodes: {}".format(len(nodes)))
        logger.info("To schedule: {}".format(len(pods_to_schedule)))

        pending_pods = self.get_pending_pods(pods_to_schedule, nodes)
        if len(pending_pods) > 0:
            scaler.fulfill_pending(pending_pods)

    def get_pods_to_schedule(self, pods, agent_pools):
        """
        given a list of KubePod objects,
        return a map of (selectors hash -> pods) to be scheduled
        """
        pending_unassigned_pods = [
            p for p in pods
            if p.status == KubePodStatus.PENDING and (not p.node_name)
        ]

        # we only consider a pod to be schedulable if it's pending and
        # unassigned and feasible
        pods_to_schedule = []
        for pod in pending_unassigned_pods:
            if capacity.is_possible(pod, agent_pools):
                pods_to_schedule.append(pod)
            else:
                logger.warn(
                    "Pending pod %s cannot fit. "
                    "Please check that requested resource amount is "
                    "consistent with node size."
                    "Scheduling skipped." % (pod.name))

        return pods_to_schedule

    def maintain(self, pods_to_schedule, running_or_pending_assigned_pods, scaler):
        scaler.maintain(pods_to_schedule, running_or_pending_assigned_pods)