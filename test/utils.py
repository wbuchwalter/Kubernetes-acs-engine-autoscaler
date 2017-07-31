import os
from autoscaler.engine_scaler import EngineScaler
from azure.cli.core.util import get_file_json

def create_scaler(nodes):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    template = get_file_json(os.path.join(dir_path, './data/azuredeploy.cluster.json'))
    parameters = get_file_json(os.path.join(dir_path, './data/azuredeploy.cluster.parameters.json'))
    return EngineScaler( 
        resource_group='my-rg',
        nodes=nodes,            
        deployments=None,
        dry_run=False,
        over_provision=0,
        spare_count=1,
        arm_parameters=parameters,
        arm_template=template,
        ignore_pools='',
        idle_threshold=0,
        notifier='')