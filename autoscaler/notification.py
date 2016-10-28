import json
import logging

import requests

logger = logging.getLogger(__name__)


def notify_scale(asg, units_requested, pods, hook=None):
    if not hook:
        logger.debug('SLACK_HOOK not configured.')
        return

    pods_string = _generate_pod_string(pods)

    message = 'ASG {}[{}] scaled up by {} to new capacity {}'.format(
        asg.name, asg.region, units_requested, asg.desired_capacity)
    message += '\n'
    message += 'Change triggered by {}'.format(pods_string)

    try:
        resp = requests.post(hook, json={
            "text": message,
            "username": "kubernetes-ec2-autoscaler",
            "icon_emoji": ":rabbit:",
        })
        logger.debug('SLACK: %s', resp.text)
    except requests.exceptions.ConnectionError as e:
        logger.critical('Failed to SLACK: %s', e)


def notify_failed_to_scale(selectors_hash, pods, hook=None):
    if not hook:
        logger.debug('SLACK_HOOK not configured.')
        return

    pods_string = _generate_pod_string(pods)

    message = 'Failed to scale {} sufficiently. Backing off...'.format(
        json.dumps(selectors_hash))
    message += '\n'
    message += 'Pods affected: {}'.format(pods_string)

    try:
        resp = requests.post(hook, json={
            "text": message,
            "username": "kubernetes-ec2-autoscaler",
            "icon_emoji": ":rabbit:",
        })
        logger.debug('SLACK: %s', resp.text)
    except requests.exceptions.ConnectionError as e:
        logger.critical('Failed to SLACK: %s', e)


def notify_invalid_pod_capacity(pod, recommended_capacity, hook=None):
    if not hook:
        logger.debug('SLACK_HOOK not configured.')
        return

    message = ("Pending pod {}/{} cannot fit {}. "
               "Please check that requested resource amount is "
               "consistent with node selectors (recommended max: {}). "
               "Scheduling skipped.".format(pod.namespace, pod.name, json.dumps(pod.selectors), recommended_capacity))

    try:
        resp = requests.post(hook, json={
            "text": message,
            "username": "kubernetes-ec2-autoscaler",
            "icon_emoji": ":rabbit:",
        })
        logger.debug('SLACK: %s', resp.text)
    except requests.exceptions.ConnectionError as e:
        logger.critical('Failed to SLACK: %s', e)


def _generate_pod_string(pods):
    if len(pods) > 5:
        pods_string = '{}, and {} others'.format(
            ', '.join('{}/{}'.format(pod.namespace, pod.name) for pod in pods[:4]),
            len(pods) - 4)
    else:
        pods_string = ', '.join('{}/{}'.format(pod.namespace, pod.name) for pod in pods)
    return pods_string
