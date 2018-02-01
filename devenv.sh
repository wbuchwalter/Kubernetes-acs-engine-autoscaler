#!/bin/bash

docker run -it -v $HOME/.kube/config:/root/.kube/config -v $(pwd):/app --entrypoint /bin/bash wbuchwalter/kubernetes-acs-engine-autoscaler:latest 