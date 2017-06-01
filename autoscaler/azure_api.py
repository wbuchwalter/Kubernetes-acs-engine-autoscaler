import requests
from adal.adal_error import AdalError
from azure.cli.core._profile import Profile
from adal.adal_error import AdalError
from azure.cli.core.prompting import prompt_pass, NoTTYException
import azure.cli.core.azlogging as azlogging
from azure.cli.core.util import CLIError
from azure.cli.core.commands.client_factory import get_mgmt_service_client
from azure.mgmt.resource.resources import ResourceManagementClient

def login(username, password, tenant):
    """Log in to access Azure subscriptions"""
    profile = Profile()
    try:
        subscriptions = profile.find_subscriptions_on_login(
            False, #interactive
            username,
            password,
            True, #is service principal
            tenant)
    except AdalError as err:
        # try polish unfriendly server errors
        msg = str(err)
        suggestion = "For cross-check, try 'az login' to authenticate through browser."
        if ('ID3242:' in msg) or ('Server returned an unknown AccountType' in msg):
            raise CLIError("The user name might be invalid. " + suggestion)
        if 'Server returned error in RSTR - ErrorCode' in msg:
            raise CLIError("Logging in through command line is not supported. " + suggestion)
        raise CLIError(err)
    except requests.exceptions.ConnectionError as err:
        raise CLIError('Please ensure you have network connection. Error detail: ' + str(err))
    all_subscriptions = list(subscriptions)
    for sub in all_subscriptions:
        sub['cloudName'] = sub.pop('environmentName', None)
    return all_subscriptions

def download_template(resource_group_name, acs_deployment):
    resource_management_client = get_mgmt_service_client(
            ResourceManagementClient)
    return resource_management_client.deployments.export_template(resource_group_name, acs_deployment).template

def download_parameters(resource_group_name, acs_deployment):
    resource_management_client = get_mgmt_service_client(
            ResourceManagementClient)
    deployment = resource_management_client.deployments.get(resource_group_name, acs_deployment)
    parameters = deployment.properties.parameters
    for parameter in parameters:
        parameters[parameter].pop('type')
    return parameters