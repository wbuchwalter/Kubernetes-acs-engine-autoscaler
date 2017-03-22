#!/bin/bash
az account clear
python main.py --container_service_name 'containerservice-kub' --resource_group 'kub' --service_principal_app_id 'xxx' --service_principal_secret 'xxxx' --service_principal_tenant_id 'xxxx' -vvv --kubeconfig /root/.kube/config --sleep 5 --over-provision 0 --debug
