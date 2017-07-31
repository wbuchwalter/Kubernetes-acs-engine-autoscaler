import hashlib
import json
import logging
import operator

from cachetools import TTLCache, cachedmethod
import json_log_formatter
import requests

logger = logging.getLogger(__name__)

struct_logger = logging.getLogger('autoscaler.notification.struct')
formatter = json_log_formatter.JSONFormatter()
handler = logging.StreamHandler()
handler.setFormatter(formatter)
struct_logger.addHandler(handler)
struct_logger.setLevel(logging.DEBUG)
struct_logger.propagate = False


def _cache_key(notifier, owner, message, pods):
    md5 = hashlib.md5()
    md5.update(owner)
    md5.update(message)

    for pod in sorted(pods, key=lambda p: p.uid):
        md5.update(pod.uid)

    key = 'v0.md5.{}'.format(md5.hexdigest())
    return key


def _generate_pod_string(pods):
    if len(pods) > 5:
        pods_string = '{}, and {} others'.format(
            ', '.join('{}/{}'.format(pod.namespace, pod.name) for pod in pods[:4]),
            len(pods) - 4)
    else:
        pods_string = ', '.join('{}/{}'.format(pod.namespace, pod.name) for pod in pods)
    return pods_string


def struct_log(message, pods, extra=None):
    for pod in pods:
        log_extra = {
            'pod_name': '{}/{}'.format(pod.namespace, pod.name),
            'pod_id': pod.uid,
            '_log_streaming_target_mapping': 'kubernetes-acs-engine-autoscaler'
        }
        if extra:
            log_extra.update(extra)
        struct_logger.debug(message, extra=log_extra)


class Notifier(object):
    MESSAGE_URL = 'https://slack.com/api/chat.postMessage'

    def __init__(self, hook=None):
        self.hook = hook

        self.cache = TTLCache(maxsize=128, ttl=60*30)

    def notify_scale(self, units_requested, pods, units_actual):
        struct_log('scale', pods,
                   extra={'units_requested': units_requested})

        if not self.hook:
            logger.debug('SLACK_HOOK not configured.')
            return

        pods_string = _generate_pod_string(pods)

        message = 'Scaled up from {} to new capacity {}'.format(
            units_actual, units_requested)
        message += '\n'
        message += 'Change triggered by {}'.format(pods_string)

        try:
            resp = requests.post(self.hook, json={
                "text": message,
                "username": "kubernetes-acs-engine-autoscaler",
                "icon_emoji": ":camel:",
            })
            logger.debug('SLACK: %s', resp.text)
        except requests.exceptions.ConnectionError as e:
            logger.critical('Failed to SLACK: %s', e)

    def notify_failed_to_scale(self, selectors_hash, pods):
        struct_log('failed to scale', pods,
                   extra={'selectors_hash': selectors_hash})

        if not self.hook:
            logger.debug('SLACK_HOOK not configured.')
            return

        pods_string = _generate_pod_string(pods)

        main_message = 'Failed to scale {} sufficiently. Backing off...'.format(
            json.dumps(selectors_hash))
        message = main_message + '\n'
        message += 'Pods affected: {}'.format(pods_string)

        try:
            resp = requests.post(self.hook, json={
                "text": message,
                "username": "kubernetes-acs-enginbe-autoscaler",
                "icon_emoji": ":camel:",
            })
            logger.debug('SLACK: %s', resp.text)
        except requests.exceptions.ConnectionError as e:
            logger.critical('Failed to SLACK: %s', e)

    def notify_invalid_pod_capacity(self, pod, recommended_capacity):
        struct_log('invalid pod capacity', [pod],
                   extra={'recommended_capacity': str(recommended_capacity)})

        if not self.hook:
            logger.debug('SLACK_HOOK not configured.')
            return

        message = ("Pending pod {}/{} cannot fit {}. "
                   "Please check that requested resource amount is "
                   "consistent with node selectors (recommended max: {}). "
                   "Scheduling skipped.".format(pod.namespace, pod.name, json.dumps(pod.selectors), recommended_capacity))

        try:
            resp = requests.post(self.hook, json={
                "text": message,
                "username": "kubernetes-acs-engine-autoscaler",
                "icon_emoji": ":camel:",
            })
            logger.debug('SLACK: %s', resp.text)
        except requests.exceptions.ConnectionError as e:
            logger.critical('Failed to SLACK: %s', e)

    def notify_drained_node(self, node, pods):
        struct_log('drain', pods, extra={'node': str(node)})

        if not self.hook:
            logger.debug('SLACK_HOOK not configured.')
            return

        pods_string = _generate_pod_string(pods)

        message = 'Node {} drained.'.format(node)
        message += '\n'
        message += 'Pod affected: {}'.format(pods_string)

        try:
            resp = requests.post(self.hook, json={
                "text": message,
                "username": "kubernetes-acs-engine-autoscaler",
                "icon_emoji": ":camel:",
            })
            logger.debug('SLACK: %s', resp.text)
        except requests.exceptions.ConnectionError as e:
            logger.critical('Failed to SLACK: %s', e)