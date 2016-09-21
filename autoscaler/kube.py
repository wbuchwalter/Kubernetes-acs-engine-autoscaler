import json
import logging

import pykube.exceptions

import autoscaler.utils as utils

logger = logging.getLogger(__name__)


class KubePodStatus(object):
    RUNNING = 'Running'
    PENDING = 'Pending'
    CONTAINER_CREATING = 'ContainerCreating'
    SUCCEEDED = 'Succeeded'
    FAILED = 'Failed'

_CORDON_LABEL = 'openai/cordoned-by-autoscaler'


class KubePod(object):
    def __init__(self, pod):
        self.original = pod

        metadata = pod.obj['metadata']
        self.name = metadata['name']
        self.namespace = metadata['namespace']
        self.node_name = pod.obj['spec'].get('nodeName', None)
        self.status = pod.obj['status']['phase']
        self.uid = metadata['uid']
        self.selectors = pod.obj['spec'].get('nodeSelector', {})
        self.labels = metadata.get('labels', {})
        self.annotations = metadata.get('annotations', {})

        # TODO: refactor
        requests = map(lambda c: c.get('resources', {}).get('requests', {}),
                       pod.obj['spec']['containers'])
        resource_requests = {}
        for d in requests:
            for k, v in d.iteritems():
                unitless_v = utils.parse_SI(v)
                resource_requests[k] = resource_requests.get(k, 0.0) + unitless_v
        self.resources = KubeResource(pods=1, **resource_requests)

    def is_mirrored(self):
        created_by = json.loads(self.annotations.get('kubernetes.io/created-by', '{}'))
        is_daemonset = created_by.get('reference', {}).get('kind') == 'DaemonSet'
        return is_daemonset or self.annotations.get('kubernetes.io/config.mirror')

    def __hash__(self):
        return hash(self.uid)

    def __eq__(self, other):
        return self.uid == other.uid

    def __str__(self):
        return 'KubePod({namespace}, {name})'.format(
            namespace=self.namespace, name=self.name)

    def __repr__(self):
        return str(self)


class KubeNode(object):
    def __init__(self, node):
        self.original = node
        self.pykube_node = node

        metadata = node.obj['metadata']
        self.name = metadata['name']
        self.instance_id, self.region, self.instance_type = self._get_instance_data()
        self.selectors = metadata['labels']

        self.capacity = KubeResource(**node.obj['status']['capacity'])
        self.used_capacity = KubeResource()
        self.unschedulable = node.obj['spec'].get('unschedulable', False)

    def _get_instance_data(self):
        """
        returns a tuple (instance id, region, instance type)
        """
        labels = self.original.obj['metadata']['labels']
        instance_type = labels.get('aws/type', labels.get('beta.kubernetes.io/instance-type'))

        provider = self.original.obj['spec'].get('providerID', '')
        if provider.startswith('aws://'):
            az, instance_id = tuple(provider.split('/')[-2:])
            return (instance_id, az[:-1], instance_type)

        if labels.get('aws/id') and labels.get('aws/az'):
            instance_id = labels['aws/id']
            region = labels['aws/az'][:-1]
            return (instance_id, region, instance_type)

        return (None, '', None)

    def uncordon(self):
        if not utils.parse_bool_label(self.selectors.get(_CORDON_LABEL)):
            logger.debug('uncordon %s ignored', self)
            return False

        try:
            self.original.reload()
            self.original.obj['spec']['unschedulable'] = False
            self.original.update()
            logger.info("uncordon %s", self)
            return True
        except pykube.exceptions.HTTPError as ex:
            logger.info("uncordon failed %s %s", self, ex)
            return False

    def cordon(self):
        try:
            self.original.reload()
            self.original.obj['spec']['unschedulable'] = True
            self.original.obj['metadata']['labels'][_CORDON_LABEL] = 'true'
            self.original.update()
            logger.info("cordon %s", self)
            return True
        except pykube.exceptions.HTTPError as ex:
            logger.info("cordon failed %s %s", self, ex)
            return False

    def delete(self):
        try:
            self.original.delete()
            logger.info("delete %s", self)
            return True
        except pykube.exceptions.HTTPError as ex:
            logger.info("delete failed %s %s", self, ex)
            return False

    def count_pod(self, pod):
        assert isinstance(pod, KubePod)
        self.used_capacity += pod.resources

    def can_fit(self, resources):
        assert isinstance(resources, KubeResource)
        left = self.used_capacity + resources - self.capacity
        return left.possible

    def is_match(self, pod):
        """
        whether this node matches all the selectors on the pod
        """
        for label, value in pod.selectors.iteritems():
            if self.selectors.get(label) != value:
                return False
        return True

    def is_managed(self):
        """
        an instance is managed if we know its instance ID in ec2.
        """
        return self.instance_id is not None

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return "{}: {} ({})".format(self.name, self.instance_id,
                                    utils.selectors_to_hash(self.selectors))


class KubeResource(object):

    def __init__(self, **kwargs):
        self.raw = dict((k, utils.parse_resource(v))
                        for (k, v) in kwargs.iteritems())

    def __add__(self, other):
        keys = set(self.raw.iterkeys()) | set(other.raw.iterkeys())
        raw_diff = dict((k, self.raw.get(k, 0) + other.raw.get(k, 0))
                        for k in keys)
        return KubeResource(**raw_diff)

    def __sub__(self, other):
        keys = set(self.raw.iterkeys()) | set(other.raw.iterkeys())
        raw_diff = dict((k, self.raw.get(k, 0) - other.raw.get(k, 0))
                        for k in keys)
        return KubeResource(**raw_diff)

    def __mul__(self, multiplier):
        new_raw = dict((k, v * multiplier) for k, v in self.raw.iteritems())
        return KubeResource(**new_raw)

    def __rmul__(self, multiplier):
        return self.__mul__(multiplier)

    def __cmp__(self, other):
        """
        should return a negative integer if self < other,
        zero if self == other, a positive integer if self > other.

        we consider self to be greater than other if it exceeds
        the resource amount in other in more resource types.
        e.g. if self = {cpu: 4, memory: 1K, gpu: 1},
        other = {cpu: 2, memory: 2K}, then self exceeds the resource
        amount in other in both cpu and gpu, while other exceeds
        the resource amount in self in only memory, so self > other.
        """
        resource_diff = (self - other).raw
        num_resource_types = len(resource_diff)
        num_eq = sum(1 for v in resource_diff.itervalues() if v == 0)
        num_less = sum(1 for v in resource_diff.itervalues() if v < 0)
        num_more = num_resource_types - num_eq - num_less
        return num_more - num_less

    def __str__(self):
        return str(self.raw)

    def get(self, key, default=None):
        return self.raw.get(key, default)

    @property
    def possible(self):
        return all(map(lambda x: x >= 0, self.raw.itervalues()))
