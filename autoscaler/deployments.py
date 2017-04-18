import logging

logger = logging.getLogger(__name__)

class Deployments:
    def __init__(self):
        self._current_deployment = None
        self.requested_pool_sizes = None
    
    def deploy(self, func, new_pool_sizes):
        if not self._current_deployment or self._current_deployment.done():
            if self.requested_pool_sizes and self.requested_pool_sizes == new_pool_sizes:
                #this can happen when a new node is coming online and kubectl isn't ready yet
                logger.info('Requested a new deployment with unchanged pool sizes, skipping.')
                return
            self.requested_pool_sizes = new_pool_sizes   
            self._current_deployment = func()            
            logger.info('Deployment started...')
            self._current_deployment.wait()
            logger.info('Deployment finished: {}'.format(self._current_deployment.result()))
        else:
            logger.info('Another deployment is already in progress')
