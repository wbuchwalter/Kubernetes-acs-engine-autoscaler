#!/bin/bash
az account clear
python main.py --container_service_name 'containerservice-kub' --resource_group 'kub' --service_principal_app_id '349a988c-8957-4f86-bb94-d4a9b366eea1' --service_principal_secret 'xxxx' --service_principal_tenant_id 'xxxx' -vvv --kubeconfig /root/.kube/config --sleep 5 --over-provision 0 --debug
