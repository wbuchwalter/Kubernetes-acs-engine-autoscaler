import datetime
import logging
import re

import botocore
from pykube.objects import Pod

import autoscaler.aws_utils as aws_utils
import autoscaler.capacity as capacity
import autoscaler.utils as utils

# we are interested in all pods, incl. system ones
Pod.objects.namespace = None

logger = logging.getLogger(__name__)


class AutoScalingGroups(object):
    _BOTO_CLIENT_TYPE = 'autoscaling'

    _CLUSTER_KEY = 'KubernetesCluster'
    _ROLE_KEYS = ('KubernetesRole', 'Role')
    _WORKER_ROLE_VALUES = ('worker', 'kubernetes-minion')

    def __init__(self, session, regions, cluster_name=None):
        """
        cluster_name - if set, filter ASGs by cluster_name in tag field
            _CLUSTER_KEY
        """
        self.session = session
        self.regions = regions
        self.cluster_name = cluster_name

    def get_all_launch_configs(self, client, raw_groups):
        all_launch_configs = {}
        batch_size = 50
        for launch_config_idx in range(0, len(raw_groups), batch_size):
            groups = raw_groups[launch_config_idx*batch_size:(launch_config_idx+1)*batch_size]
            kwargs = {
                'LaunchConfigurationNames': [g['LaunchConfigurationName'] for g in groups]
            }
            launch_configs = aws_utils.fetch_all(
                client.describe_launch_configurations,
                kwargs, 'LaunchConfigurations')
            all_launch_configs.update((lc['LaunchConfigurationName'], lc)
                                      for lc in launch_configs)
        return all_launch_configs

    def get_all_groups(self, kube_nodes):
        groups = []
        for region in self.regions:
            client = self.session.client(self._BOTO_CLIENT_TYPE,
                                         region_name=region)
            raw_groups = aws_utils.fetch_all(
                client.describe_auto_scaling_groups, {}, 'AutoScalingGroups')

            launch_configs = self.get_all_launch_configs(client, raw_groups)

            for raw_group in sorted(raw_groups, key=lambda g: g['AutoScalingGroupName']):
                if self.cluster_name:
                    cluster_name = None
                    role = None
                    for tag in raw_group['Tags']:
                        if tag['Key'] == self._CLUSTER_KEY:
                            cluster_name = tag['Value']
                        elif tag['Key'] in self._ROLE_KEYS:
                            role = tag['Value']
                    if cluster_name != self.cluster_name or role not in self._WORKER_ROLE_VALUES:
                        continue

                groups.append(AutoScalingGroup(
                    client, region, kube_nodes, raw_group,
                    launch_configs[raw_group['LaunchConfigurationName']]))

        return groups


class AutoScalingTimeouts(object):
    _TIMEOUT = 3600  # 1 hour
    _SPOT_REQUEST_TIMEOUT = 300  # 5 minutes
    _MAX_OUTBIDS_IN_INTERVAL = 60*20  # 20 minutes
    _SPOT_HISTORY_PERIOD = 60*60*5  # 5 hours

    def __init__(self, session):
        """
        """
        self.session = session

        # ASGs to avoid because of recent launch failures
        # e.g. a region running out of capacity
        # try to favor other regions
        self._timeouts = {}
        self._last_activities = {}

        # ASGs to avoid because of spot pricing history
        self._spot_timeouts = {}

    def refresh_timeouts(self, asgs, dry_run=False):
        """
        refresh timeouts on ASGs using new data from aws
        """
        self.time_out_spot_asgs(asgs)
        for asg in asgs:
            self.reconcile_limits(asg, dry_run=dry_run)

    def reconcile_limits(self, asg, dry_run=False):
        """
        makes sure the ASG has valid capacity by processing errors
        in its recent scaling activities.
        marks an ASG as timed out if it recently had a capacity
        failure.
        """
        start_time_cutoff = None
        for entry in asg.iter_activities():
            if entry['ActivityId'] == self._last_activities.get(asg._id):
                # already processed the rest
                return
            if start_time_cutoff is None:
                # won't process this activity twice
                self._last_activities[asg._id] = entry['ActivityId']
                start_time_cutoff = (
                    datetime.datetime.now(entry['StartTime'].tzinfo) -
                    datetime.timedelta(seconds=self._TIMEOUT))
            if entry['StartTime'] < start_time_cutoff:
                # skip events that are too old to cut down the time
                # it takes the first time to go through events
                break

            # events that have partial progress are not considered processed
            if entry['Progress'] < 100:
                start_time_cutoff = None

            status_msg = entry.get('StatusMessage', '')
            if entry['StatusCode'] in ('Failed', 'Cancelled'):
                logger.warn('%s scaling failure: %s', asg, entry)

                m = AutoScalingErrorMessages.INSTANCE_LIMIT.match(status_msg)
                if m:
                    max_desired_capacity = int(m.group('requested')) - 1
                    if asg.desired_capacity > max_desired_capacity:
                        self._timeouts[asg._id] = entry['StartTime'] + datetime.timedelta(seconds=self._TIMEOUT)
                        logger.info('%s is timed out until %s',
                                    asg.name, self._timeouts[asg._id])

                        # we tried to go over capacity and failed
                        # now set the desired capacity back to a normal range
                        if not dry_run:
                            asg.set_desired_capacity(max_desired_capacity)
                        else:
                            logger.info('[Dry run] Would have set desired capacity to %s', max_desired_capacity)
                    return

                m = AutoScalingErrorMessages.VOLUME_LIMIT.match(status_msg)
                if m:
                    # TODO: decrease desired capacity
                    self._timeouts[asg._id] = entry['StartTime'] + datetime.timedelta(seconds=self._TIMEOUT)
                    logger.info('%s is timed out until %s',
                                asg.name, self._timeouts[asg._id])
                    return

                m = AutoScalingErrorMessages.CAPACITY_LIMIT.match(status_msg)
                if m:
                    # try to decrease desired capacity
                    cause_m = AutoScalingCauseMessages.LAUNCH_INSTANCE.search(entry.get('Cause', ''))
                    if cause_m:
                        original_capacity = int(cause_m.group('original_capacity'))
                        if asg.desired_capacity > original_capacity:
                            self._timeouts[asg._id] = entry['StartTime'] + datetime.timedelta(seconds=self._TIMEOUT)
                            logger.info('%s is timed out until %s',
                                        asg.name, self._timeouts[asg._id])

                            # we tried to go over capacity and failed
                            # now set the desired capacity back to a normal range
                            if not dry_run:
                                asg.set_desired_capacity(original_capacity)
                            else:
                                logger.info('[Dry run] Would have set desired capacity to %s', original_capacity)
                    return

                m = AutoScalingErrorMessages.SPOT_REQUEST_CANCELLED.search(status_msg)
                if m:
                    # we cancelled a spot request
                    # don't carry on to reset timeout
                    continue

                m = AutoScalingErrorMessages.SPOT_LIMIT.match(status_msg)
                if m:
                    self._timeouts[asg._id] = entry['StartTime'] + datetime.timedelta(seconds=self._TIMEOUT)
                    logger.info('%s is timed out until %s',
                                asg.name, self._timeouts[asg._id])

                    if not dry_run:
                        asg.set_desired_capacity(asg.actual_capacity)
                    else:
                        logger.info('[Dry run] Would have set desired capacity to %s', asg.actual_capacity)
                    return
            elif entry['StatusCode'] == 'WaitingForSpotInstanceId':
                logger.warn('%s waiting for spot: %s', asg, entry)
                now = datetime.datetime.now(entry['StartTime'].tzinfo)
                if (now - entry['StartTime']) > datetime.timedelta(seconds=self._SPOT_REQUEST_TIMEOUT):
                    self._timeouts[asg._id] = entry['StartTime'] + datetime.timedelta(seconds=self._TIMEOUT)
                    logger.info('%s is timed out until %s',
                                asg.name, self._timeouts[asg._id])

                    # try to cancel spot request and scale down ASG
                    spot_request_m = AutoScalingErrorMessages.SPOT_REQUEST_WAITING.search(status_msg)
                    if spot_request_m:
                        spot_request_id = spot_request_m.group('request_id')
                        if not dry_run:
                            cancelled = self.cancel_spot_request(asg.region, spot_request_id)
                            if cancelled:
                                asg.set_desired_capacity(asg.desired_capacity - 1)
                        else:
                            logger.info('[Dry run] Would have cancelled spot request %s and decremented desired capacity.',
                                        spot_request_id)
                    # don't return here so that we can cancel more spot requests

        self._timeouts[asg._id] = None
        logger.debug('%s has no timeout', asg.name)

    def is_timed_out(self, asg):
        timeout = self._timeouts.get(asg._id)
        spot_timeout = self._spot_timeouts.get(asg._id)

        if timeout and datetime.datetime.now(timeout.tzinfo) < timeout:
            return True

        if spot_timeout and datetime.datetime.utcnow() < spot_timeout:
            return True

        return False

    def cancel_spot_request(self, region, request_id):
        client = self.session.client('ec2',
                                     region_name=region)
        response = client.describe_spot_instance_requests(
            SpotInstanceRequestIds=[request_id]
        )
        if len(response['SpotInstanceRequests']) == 0:
            return False

        spot_instance_req = response['SpotInstanceRequests'][0]
        if spot_instance_req['State'] in ('open', 'active'):
            response = client.cancel_spot_instance_requests(
                SpotInstanceRequestIds=[request_id]
            )
            return True

        return False

    def time_out_spot_asgs(self, asgs):
        """
        Using recent spot pricing data from AWS, time out spot instance
        ASGs that would be outbid for more than _MAX_OUTBIDS_IN_INTERVAL seconds
        """
        region_instance_asg_map = {}
        for asg in asgs:
            if not asg.is_spot:
                continue

            instance_asg_map = region_instance_asg_map.setdefault(asg.region, {})
            instance_type = asg.launch_config['InstanceType']
            instance_asg_map.setdefault(instance_type, []).append(asg)

        now = datetime.datetime.utcnow()
        since = now - datetime.timedelta(seconds=self._SPOT_HISTORY_PERIOD)

        for region, instance_asg_map in region_instance_asg_map.items():
            client = self.session.client('ec2', region_name=region)
            kwargs = {
                'StartTime': since,
                'InstanceTypes': instance_asg_map.keys(),
                'ProductDescriptions': ['Linux/UNIX']
            }
            history = aws_utils.fetch_all(
                client.describe_spot_price_history, kwargs, 'SpotPriceHistory')
            for instance_type, asgs in instance_asg_map.items():
                for asg in asgs:
                    last_az_bid = {}
                    outbid_time = {}
                    bid_price = float(asg.launch_config['SpotPrice'])
                    for item in history:
                        if item['InstanceType'] != instance_type:
                            continue

                        if float(item['SpotPrice']) > bid_price:
                            # we would've been outbid!
                            if item['AvailabilityZone'] in last_az_bid:
                                time_diff = (last_az_bid[item['AvailabilityZone']] - item['Timestamp'])
                            else:
                                time_diff = datetime.timedelta(seconds=0)
                            outbid_time[item['AvailabilityZone']] = (
                                outbid_time.get(item['AvailabilityZone'], datetime.timedelta(seconds=0)) +
                                time_diff)
                        last_az_bid[item['AvailabilityZone']] = item['Timestamp']

                    if outbid_time:
                        avg_outbid_time = sum(t.total_seconds() for t in outbid_time.values()) / len(outbid_time)
                    else:
                        avg_outbid_time = 0.0
                    if avg_outbid_time > self._MAX_OUTBIDS_IN_INTERVAL:
                        self._spot_timeouts[asg._id] = now + datetime.timedelta(seconds=self._TIMEOUT)
                        logger.info('%s (%s) is spot timed out until %s (would have been outbid for %ss on average)',
                                    asg.name, asg.region, self._spot_timeouts[asg._id], avg_outbid_time)
                    else:
                        self._spot_timeouts[asg._id] = None


class AutoScalingGroup(object):
    def __init__(self, client, region, kube_nodes, raw_group, launch_config):
        """
        client - boto3 AutoScaling.Client
        region - AWS region string
        kube_nodes - list of KubeNode objects
        raw_group - raw ASG dictionary returned from AWS API
        launch_config - raw launch config dictionary returned from AWS API
        """
        self.client = client
        self.region = region
        self.launch_config = launch_config
        self.selectors = self._extract_selectors(region, launch_config, raw_group['Tags'])
        self.name = raw_group['AutoScalingGroupName']
        self.desired_capacity = raw_group['DesiredCapacity']
        self.min_size = raw_group['MinSize']
        self.max_size = raw_group['MaxSize']

        self.is_spot = launch_config.get('SpotPrice') is not None

        self.instance_ids = set(inst['InstanceId'] for inst in raw_group['Instances']
                                if inst.get('InstanceId'))
        self.nodes = [node for node in kube_nodes
                      if node.instance_id in self.instance_ids]
        self.unschedulable_nodes = filter(
            lambda n: n.unschedulable, self.nodes)

        self._id = (self.region, self.name)

    def _extract_selectors(self, region, launch_config, tags_data):
        selectors = {
            'aws/type': launch_config['InstanceType'],
            'aws/class': launch_config['InstanceType'][0],
            'aws/ami-id': launch_config['ImageId'],
            'aws/region': region
        }
        for tag_data in tags_data:
            if tag_data['Key'].startswith('kube/'):
                selectors[tag_data['Key'][5:]] = tag_data['Value']

        # adding kube label counterparts
        selectors['beta.kubernetes.io/instance-type'] = selectors['aws/type']
        selectors['failure-domain.beta.kubernetes.io/region'] = selectors['aws/region']

        return selectors

    @property
    def max_resource_capacity(self):
        return self.max_size * capacity.get_unit_capacity(self)

    @property
    def actual_capacity(self):
        return len(self.nodes)

    def set_desired_capacity(self, new_desired_capacity):
        """
        sets the desired capacity of the underlying ASG directly.
        note that this is for internal control.
        for scaling purposes, please use scale() instead.
        """
        logger.info("ASG: {} new_desired_capacity: {}".format(
            self, new_desired_capacity))
        self.client.set_desired_capacity(AutoScalingGroupName=self.name,
                                         DesiredCapacity=new_desired_capacity,
                                         HonorCooldown=False)
        self.desired_capacity = new_desired_capacity

    def scale(self, new_desired_capacity):
        """
        scales the ASG to the new desired capacity.
        returns True if desired capacity has been increased as a result.
        """
        desired_capacity = min(self.max_size, new_desired_capacity)
        num_unschedulable = len(self.unschedulable_nodes)
        num_schedulable = self.actual_capacity - num_unschedulable

        logger.info("Desired {}, currently at {}".format(
            desired_capacity, self.desired_capacity))
        logger.info("Kube node: {} schedulable, {} unschedulable".format(
            num_schedulable, num_unschedulable))

        # Try to get the number of schedulable nodes up if we don't have enough, regardless of whether
        # group's capacity is already at the same as the desired.
        if num_schedulable < desired_capacity:
            for node in self.unschedulable_nodes:
                if node.uncordon():
                    num_schedulable += 1
                    # Uncordon only what we need
                    if num_schedulable == desired_capacity:
                        break

        if self.desired_capacity != desired_capacity:
            if self.desired_capacity == self.max_size:
                logger.info("Desired same as max, desired: {}, schedulable: {}".format(
                    self.desired_capacity, num_schedulable))
                return False

            scale_up = self.desired_capacity < desired_capacity
            # This should be a rare event
            # note: this micro-optimization is not worth doing as the race condition here is
            #    tricky. when ec2 initializes some nodes in the meantime, asg will shutdown
            #    nodes by its own policy
            # scale_down = self.desired_capacity > desired_capacity >= self.actual_capacity
            if scale_up:
                # should have gotten our num_schedulable to highest value possible
                # actually need to grow.
                self.set_desired_capacity(desired_capacity)
                return True

        logger.info("Doing nothing: desired_capacity correctly set: {}, schedulable: {}".format(
            self.name, num_schedulable))
        return False

    def scale_node_in(self, node):
        """
        scale down asg by terminating the given node.
        returns True if node was successfully terminated.
        """
        try:
            # if we somehow end up in a situation where we have
            # more capacity than desired capacity, and the desired
            # capacity is at asg min size, then when we try to
            # terminate the instance while decrementing the desired
            # capacity, the aws api call will fail
            decrement_capacity = self.desired_capacity > self.min_size
            self.client.terminate_instance_in_auto_scaling_group(
                InstanceId=node.instance_id,
                ShouldDecrementDesiredCapacity=decrement_capacity)
            self.nodes.remove(node)
            logger.info('Scaled node %s in', node)
        except botocore.exceptions.ClientError as e:
            if str(e).find("Terminating instance without replacement will "
                           "violate group's min size constraint.") == -1:
                raise e
            logger.error("Failed to terminate instance: %s", e)
            return False

        return True

    def iter_activities(self):
        next_token = None
        while True:
            kwargs = {
                'AutoScalingGroupName': self.name,
                'MaxRecords': 10
            }
            if next_token:
                kwargs['NextToken'] = next_token
            data = self.client.describe_scaling_activities(**kwargs)
            for item in data['Activities']:
                yield item
            next_token = data.get('NextToken')
            if not next_token:
                break

    def contains(self, node):
        return node.instance_id in self.instance_ids

    def is_match_for_selectors(self, selectors):
        for label, value in selectors.items():
            if self.selectors.get(label) != value:
                return False
        return True

    def __str__(self):
        return 'AutoScalingGroup({name}, {selectors_hash})'.format(name=self.name, selectors_hash=utils.selectors_to_hash(self.selectors))

    def __repr__(self):
        return str(self)


class AutoScalingErrorMessages(object):
    INSTANCE_LIMIT = re.compile(r'You have requested more instances \((?P<requested>\d+)\) than your current instance limit of (?P<limit>\d+) allows for the specified instance type. Please visit http://aws.amazon.com/contact-us/ec2-request to request an adjustment to this limit. Launching EC2 instance failed.')
    VOLUME_LIMIT = re.compile(r'Instance became unhealthy while waiting for instance to be in InService state. Termination Reason: Client.VolumeLimitExceeded: Volume limit exceeded')
    CAPACITY_LIMIT = re.compile(r'Insufficient capacity\. Launching EC2 instance failed\.')
    SPOT_REQUEST_WAITING = re.compile(r'Placed Spot instance request: (?P<request_id>.+). Waiting for instance\(s\)')
    SPOT_REQUEST_CANCELLED = re.compile(r'Spot instance request: (?P<request_id>.+) has been cancelled\.')
    SPOT_LIMIT = re.compile(r'Max spot instance count exceeded\. Placing Spot instance request failed\.')


class AutoScalingCauseMessages(object):
    LAUNCH_INSTANCE = re.compile(r'At \d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ an instance was started in response to a difference between desired and actual capacity, increasing the capacity from (?P<original_capacity>\d+) to (?P<target_capacity>\d+)\.')
