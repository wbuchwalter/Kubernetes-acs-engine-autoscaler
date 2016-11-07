import hashlib
import json
import logging
import operator

from cachetools import TTLCache, cachedmethod
import requests

logger = logging.getLogger(__name__)


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


class Notifier(object):
    MESSAGE_URL = 'https://slack.com/api/chat.postMessage'

    def __init__(self, hook=None, bot_token=None):
        self.hook = hook
        self.bot_token = bot_token

        self.cache = TTLCache(maxsize=128, ttl=60*30)

    def message_owners(self, message, pods):
        if not self.bot_token:
            logger.debug('SLACK_BOT_TOKEN not configured.')
            return

        pods_by_owner = {}
        for pod in pods:
            if pod.owner:
                pods_by_owner.setdefault(pod.owner, []).append(pod)

        for owner, pods in pods_by_owner.items():
            self.message_owner(owner, message, pods)

    def notify_scale(self, asg, units_requested, pods):
        if not self.hook:
            logger.debug('SLACK_HOOK not configured.')
            return

        pods_string = _generate_pod_string(pods)

        message = 'ASG {}[{}] scaled up by {} to new capacity {}'.format(
            asg.name, asg.region, units_requested, asg.desired_capacity)
        message += '\n'
        message += 'Change triggered by {}'.format(pods_string)

        try:
            resp = requests.post(self.hook, json={
                "text": message,
                "username": "kubernetes-ec2-autoscaler",
                "icon_emoji": ":rabbit:",
            })
            logger.debug('SLACK: %s', resp.text)
        except requests.exceptions.ConnectionError as e:
            logger.critical('Failed to SLACK: %s', e)

        self.message_owners(
            'ASG {}[{}] scaled up'.format(asg.name, asg.region), pods)

    def notify_failed_to_scale(self, selectors_hash, pods):
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
                "username": "kubernetes-ec2-autoscaler",
                "icon_emoji": ":rabbit:",
            })
            logger.debug('SLACK: %s', resp.text)
        except requests.exceptions.ConnectionError as e:
            logger.critical('Failed to SLACK: %s', e)

        self.message_owners(main_message, pods)

    def notify_invalid_pod_capacity(self, pod, recommended_capacity):
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
                "username": "kubernetes-ec2-autoscaler",
                "icon_emoji": ":rabbit:",
            })
            logger.debug('SLACK: %s', resp.text)
        except requests.exceptions.ConnectionError as e:
            logger.critical('Failed to SLACK: %s', e)

        self.message_owners(message, [pod])

    @cachedmethod(operator.attrgetter('cache'), key=_cache_key)
    def message_owner(self, owner, message, pods):
        attachments = [{
            'pretext': 'Affected pods',
            'text': ', '.join('{}/{}'.format(pod.namespace, pod.name) for pod in pods)
        }]

        try:
            resp = requests.post(self.MESSAGE_URL, data={
                "text": message,
                "attachments": json.dumps(attachments),
                "token": self.bot_token,
                "channel": "@{}".format(owner),
                "username": "kubernetes-ec2-autoscaler",
                "icon_emoji": ":rabbit:",
            })
            logger.debug('SLACK: %s', resp.text)
        except requests.exceptions.RequestException as e:
            logger.critical('Failed to SLACK: %s', e)
