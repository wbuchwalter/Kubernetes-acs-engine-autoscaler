import requests
import logging
import azure.cli.core.azlogging as azlogging
from azure.cli.core.util import CLIError
from azure.cli.core.profiles import ResourceType
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlockBlobService
from azure.common import AzureHttpError

logger = logging.getLogger(__name__)
resource_management_client = None
compute_management_client = None
storage_management_client = None

def login(username, password, tenant, subscriptionId):
    from azure.common.client_factory import get_client_from_json_dict
    global resource_management_client
    global compute_management_client
    global storage_management_client

    config_dict = {
        "clientId": username,
        "clientSecret": password,
        "subscriptionId": subscriptionId,
        "tenantId": tenant,
        "activeDirectoryEndpointUrl": "https://login.microsoftonline.com",
        "resourceManagerEndpointUrl": "https://management.azure.com/",
        "activeDirectoryGraphResourceId": "https://graph.windows.net/",
        "sqlManagementEndpointUrl": "https://management.core.windows.net:8443/",
        "galleryEndpointUrl": "https://gallery.azure.com/",
        "managementEndpointUrl": "https://management.core.windows.net/"
    }
    resource_management_client = get_client_from_json_dict(ResourceManagementClient, config_dict)
    compute_management_client = get_client_from_json_dict(ComputeManagementClient, config_dict)
    storage_management_client = get_client_from_json_dict(StorageManagementClient, config_dict)
    
def download_template(resource_group_name, acs_deployment):
    return resource_management_client.deployments.export_template(resource_group_name, acs_deployment).template

def download_parameters(resource_group_name, acs_deployment):
    deployment = resource_management_client.deployments.get(resource_group_name, acs_deployment)
    parameters = deployment.properties.parameters
    for parameter in parameters:
        parameters[parameter].pop('type')
    return parameters

def create_deployment(resource_group_name, deployment_name, properties):
    return resource_management_client.deployments.create_or_update(resource_group_name,
                deployment_name,
                properties, raw=False)

def delete_resources_for_node(node, resource_group_name):
    global resource_management_client
    global compute_management_client
    global storage_management_client

    logger.info('deleting node {}'.format(node.name))

    vm_details = compute_management_client.virtual_machines.get(
        resource_group_name, node.name, None)
    os_disk = vm_details.storage_profile.os_disk

    managed_disk_name = None
    account_name = None
    container_name = None
    blob_name = None

    # save disk location
    if os_disk.managed_disk:
        managed_disk_name = os_disk.name
    else:
        storage_infos = vm_details.storage_profile.os_disk.vhd.uri.split('/')
        account_name = storage_infos[2].split('.')[0]
        container_name = storage_infos[3]
        blob_name = storage_infos[4]

    # delete vm
    logger.info('Deleting VM for {}'.format(node.name))
    delete_vm_op = resource_management_client.resources.delete(resource_group_name,
                                                                'Microsoft.Compute',
                                                                '',
                                                                'virtualMachines',
                                                                node.name,
                                                                '2016-03-30')
    delete_vm_op.wait()

    # delete nic
    logger.info('Deleting NIC for {}'.format(node.name))
    name_parts = node.name.split('-')
    nic_name = '{}-{}-{}-nic-{}'.format(
        name_parts[0], name_parts[1], name_parts[2], name_parts[3])
    delete_nic_op = resource_management_client.resources.delete(resource_group_name,
                                                                'Microsoft.Network',
                                                                '',
                                                                'networkInterfaces',
                                                                nic_name,
                                                                '2016-03-30')
    delete_nic_op.wait()
    
    # delete os blob
    logger.info('Deleting OS disk for {}'.format(node.name))
    if os_disk.managed_disk:
        delete_managed_disk_op = compute_management_client.disks.delete(resource_group_name, managed_disk_name)
        delete_managed_disk_op.wait()
    else:        
        keys = storage_management_client.storage_accounts.list_keys(
            resource_group_name, account_name)
        key = keys.keys[0].value

        for i in range(5):
            try:
                block_blob_service = BlockBlobService(
                    account_name=account_name, account_key=key)
                block_blob_service.delete_blob(container_name, blob_name)
            except AzureHttpError as err:
                print(err.message)
                continue
            break