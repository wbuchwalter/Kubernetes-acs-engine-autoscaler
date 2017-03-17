#!/bin/bash

python main.py --container_service_name 'containerservice-kub' --resource_group 'kub' --service_principal_app_id '349a988c-8957-4f86-bb94-d4a9b366eea1' --service_principal_secret '31115669-1158-41cc-bb1b-1c207be38779' --service_principal_tenant_id '72f988bf-86f1-41af-91ab-2d7cd011db47' --dry-run -vvv --kubeconfig /root/.kube/config