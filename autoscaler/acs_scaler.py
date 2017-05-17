from azure.cli.core.commands.client_factory import get_mgmt_service_client
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlockBlobService
import time
import logging
import autoscaler.utils as utils
from autoscaler.agent_pool import AgentPool


logger = logging.getLogger(__name__)


# Scaler for ACS, not implemented yet

class ContainerService(object):

    def __init__(
        self, resource_group, nodes, deployments, dry_run, over_provision,
        container_service_name):
        
        raise NotImplementedError()

