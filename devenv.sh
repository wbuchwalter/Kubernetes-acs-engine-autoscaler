#!/bin/bash

docker run -it -v $HOME/.kube/config:/root/.kube/config -v $(pwd):/app wbuchwalter/autoscaler /bin/bash