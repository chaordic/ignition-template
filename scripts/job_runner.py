#!/usr/bin/env python
# This is script that shows how to use the Ignition tools to run a cluster using spots machine but with automatic recovery
# It is a supervisor that guarantees to:
# 1) Pick the best configuration based on spot price (it also supports ondemand machines)
# 2) Launch the cluster
# 3) Verify the cluster is healthy through a sanity check
# 4) Run the job one or more times
# 5) If the job fails, verify again the cluster health
# 6) Recreate the cluster if unhealthy or just the job again if healthy (up to max_errors_on_healthy_cluster)
# 7) Destroy the cluster if the job is of type "run once"
# 8) Log all the problems to VictorOps (http://victorops.com/) and send a heartbeat to Librato (https://www.librato.com/)

# You can call this script from cron or a system service
# See start_mysetup1_job_runner.sh for a example on how to call this script
import os, sys, collections, itertools, logging, traceback, subprocess, datetime, base64, pickle, time, uuid, json
import boto.ec2
import librato
import requests
script_path = os.path.dirname(os.path.realpath(__file__))
# We expect some conventional structure:
# It will read a user data file localized in the same directory as the script
# This user data is a script that runs on each machine initialization. It can be used to configure stuff like log rotation
user_data_script = os.path.join(script_path, 'user_data.sh')
# We will import the cluster.py, which we expect to be in a path relative to this script
# E.g if job_runner is at root/scripts/job_runner.py, cluster.py must be at root/core/tools/cluster.py
sys.path.insert(0, os.path.join(script_path, '..', 'core', 'tools'))
import cluster


## Pseudo-types defnition

ClusterConf = collections.namedtuple('ClusterConf', ['instance_type', 'spot_price', 'slaves', 'worker_instances', 'job_mem', 'ami_type'])
RegionConf = collections.namedtuple('RegionConf', ['region', 'ami_pvm', 'ami_hvm', 'az_suffixes', 'vpc', 'subnet_by_az'])
FullConf = collections.namedtuple('FullConf', ['cluster_conf', 'region_conf', 'az'])

## Constants

env = 'prod'
default_security_group = 'spark-cluster-sg' # This can and should be overriden by the arguments on command line 
# Check if this setup exists and really works. It should be a fast but reasonably complete test. 
# We use a job that reads bigs files from s3, syncs it to hdfs then make a count
sanity_check_job_name = 'HelloWorldSetup' 
sanity_check_job_timeout_minutes = 7
# This may change fast, see the default in cluster.py for the best version
spark_version = 'https://circle-artifacts.com/gh/chaordic/spark/3/artifacts/0/tmp/circle-artifacts.zAWvGZt/spark-1.2.2-SNAPSHOT-bin-1.0.4.tgz'

job_timeout_minutes = 240
# Max number of consecutive errors on a healthy cluster before forcing cluster destruction
# For instance, this may happen if the disk is almost full, but not full enough to fail the sanity checks
max_errors_on_healthy_cluster = 5000 # Change this to a low number to enable it

# If you are going to use VPC, change below
# The AMI is per region, you can see some AMI IDs here:
# https://github.com/chaordic/spark-ec2/tree/v3/ami-list
regions_conf = collections.OrderedDict([
    ('us-east-1', RegionConf('us-east-1', 'ami-5bb18832', 'ami-35b1885c', ['a', 'b', 'c', 'e'], '<vpc-your_id>',
        collections.OrderedDict([
            ('us-east-1a', '<subnet-your_id>'),
            ('us-east-1b', '<subnet-your_id>'),
            ('us-east-1c', '<subnet-your_id>'),
            ('us-east-1e', '<subnet-your_id>')])
        )
    ),
    # This scripts support multi-regions, but currently it will pick the best configuration in the given list of regions
    # This may be undesirable due to data transfer costs. Add other regions with caution
])

master_ami_type = 'pvm'
master_instance_type = 'm3.2xlarge'

runner_eid = "runner-{0}".format(uuid.uuid4())
victorops_url = "https://alert.victorops.com/integrations/generic/20131114/alert/<you_key>/<your_tag>"


## LOGGING

log = logging.getLogger()
log.setLevel(logging.INFO)
#log.setLevel(logging.DEBUG)
formatter = logging.Formatter('job-runner - %(asctime)s - %(levelname)s - %(message)s')
handler = log.handlers[0]
handler.setFormatter(formatter)
log.addHandler(handler)


## Cluster Configuration

mysetup1_cluster_sequence = [
    ClusterConf('r3.8xlarge', 0.42 * 3, '8', '4', '30G', 'hvm'),
    ClusterConf('r3.4xlarge', 0.21 * 3, '15', '2', '40G', 'hvm'),
    ClusterConf('r3.2xlarge', 0.12 * 3, '30', '1', '40G', 'hvm'),
    ClusterConf('r3.xlarge', 0.06 * 3, '60', '1', '20G', 'hvm'),
    ClusterConf('hi1.4xlarge', 0.26 * 3, '15', '1', '40G', 'hvm'),
    ClusterConf('c3.8xlarge', 0.42 * 2, '10', '1', '40G', 'hvm'),
    ClusterConf('m2.4xlarge', 0.12 * 4, '70', '1', '40G', 'pvm'),
    ClusterConf('c3.4xlarge', 0.26 * 2, '20', '1', '20G', 'hvm'),
    ClusterConf('m3.2xlarge', 0.12 * 2, '40', '1', '20G', 'pvm'),

    # On demand confs
    #ClusterConf('r3.8xlarge', None, '5', '8', '20G', 'hvm'),
    #ClusterConf('r3.4xlarge', None, '10', '4', '20G', 'hvm'),
    #ClusterConf('r3.2xlarge', None, '20', '2', '20G', 'hvm'),
    #ClusterConf('r3.xlarge', None, '40', '1', '20G', 'hvm'),
]


mysetup2_cluster_sequence = [
    ClusterConf('r3.2xlarge', 0.12 * 3, '8', '2', '15G', 'hvm'),
    ClusterConf('r3.xlarge', 0.06 * 3, '16', '1', '15G', 'hvm'),
    ClusterConf('r3.4xlarge', 0.21 * 3, '4', '4', '15G', 'hvm'),
    ClusterConf('r3.8xlarge', 0.42 * 3, '2', '8', '15G', 'hvm'),
]

namespace = "runner"

# This is a heart beat so we know the script is running. Currently we send it to librato
def send_heartbeat(retries=3):
    for i in range(retries):
        try:
            user = '<your_user>'
            token = '<your_token>'
            api = librato.connect(user, token)
            api.submit('runner.{0}.heartbeat'.format(namespace), 1)
            break
        except Exception as e:
            log.exception('Exception sending health check to librato')
            notify("Exception sending health check to librato\n\nException is: " + traceback.format_exc(), severity="WARNING")
            time.sleep(1)

# Every problem is sent to VictorOps. Some issues will be automatically be resolved by the script if it get to auto-recover from the problem
def victorops_alert(entity_id, severity="CRITICAL", message=""):
    message = {
        "message_type": severity,
        "entity_id": entity_id,
        "timestamp": int(time.time()),
        "state_message": message,
    }

    r = requests.post(victorops_url, data=json.dumps(message))
    return r.text

def notify(message, severity="CRITICAL", entity_id=runner_eid):
    try:
        return victorops_alert(entity_id, severity, message)
    except Exception as e:
        log.exception('This is bad, notification failed. Someone must be notified about this.')


def avg(it):
    l = list(it)
    return sum(l) / len(l) if len(l) > 0 else 0

def get_avg_worst_price(hprices, n):
    return avg(sorted([h.price for h in hprices], reverse=True)[:n])


def get_date_start_at(hours_past, now=None):
    """
    >>> get_date_start_at(7, datetime.datetime(2014, 5, 1, 0, 0, 0))
    '2014-04-30T17:00:00.000Z'
    """
    now = now or datetime.datetime.utcnow()
    return (now - datetime.timedelta(hours=hours_past)).isoformat()[:19] + '.000Z'


def get_prices_after_hours_ago(hprices, hours):
    timestamp = get_date_start_at(hours)
    return [h for h in hprices if h.timestamp >= timestamp]

def get_azs_for_region(region_conf):
    return [region_conf.region + a for a in region_conf.az_suffixes]


# This is the main algorithm to pick a configuration. It may be changed to suit different needs
def get_best_conf_and_az(blacklisted_confs, cluster_sequence, safe_price_margin = 0.8, entity_id=runner_eid):
    def inotify(message, severity="CRITICAL"):
        return notify(message, severity, entity_id=entity_id)

    while True:
        send_heartbeat()
        log.info('Getting spot price history')
        try:
            for cluster_conf in cluster_sequence:
                if cluster_conf.spot_price is not None:
                    price_per_full_conf = []
                    for region, region_conf in regions_conf.items():
                        conn = boto.ec2.connect_to_region(region)
                        for az in get_azs_for_region(region_conf):
                            try:
                                prices = conn.get_spot_price_history(instance_type=cluster_conf.instance_type, availability_zone=az, product_description='Linux/UNIX')
                            except Exception as e2:
                                continue
                            # Get the avg of the 10 worst prices in last week
                            average_worst_historical_price = get_avg_worst_price(get_prices_after_hours_ago(prices, 12), 10)
                            # Get the prices from approx. the last 6 hours and average the two worst
                            average_worst_recent_price = get_avg_worst_price(get_prices_after_hours_ago(prices, 6), 2)
                            if average_worst_recent_price > 0 or average_worst_historical_price > 0:
                                log.info('Price (recent, historical) for instance {0} in {1} is ({2}, {3})'.format(cluster_conf.instance_type, az, average_worst_recent_price, average_worst_historical_price))
                                # We pick the worst price
                                price = max(average_worst_recent_price, average_worst_historical_price)
                                full_conf = FullConf(cluster_conf, region_conf, az)
                                if full_conf not in blacklisted_confs:
                                    price_per_full_conf.append((price, full_conf))
                                else:
                                    log.info("Ignoring this conf because it's blacklisted for now: {0}".format(blacklisted_confs))
                    # price_per_full_conf is a list [ (price1, fullconf1), ...]
                    # sort: cheaper regions/azs first
                    price_per_full_conf.sort()
                    maximum_accepted_price = float(cluster_conf.spot_price) * safe_price_margin
                    if price_per_full_conf and price_per_full_conf[0][0] < maximum_accepted_price:
                        full_conf = price_per_full_conf[0][1]
                        log.info('Best conf is: {0}, with average price {1}'.format(full_conf, price_per_full_conf[0][0]))
                        if full_conf.cluster_conf != cluster_sequence[0]:
                            if cluster_conf == cluster_sequence[-1]:
                                inotify('Urgent: Imminent cluster failure' "\n" 'We are using our last cluster option. The processing is close to a halt if this cluster fails. By the way, this cluster is expensive and potentially slow.')
                            elif cluster_sequence.index(full_conf.cluster_conf) >= len(cluster_sequence) // 2:
                                inotify('Spot problems __right now__' "\n" 'We are using one of our last cluster configurations. There is a risk we may face a halt in the processing if the problem persists and no action is taken.')
                            else:
                                inotify('Spot problems ahead' "\n" 'We are not running on our first cluster choice. This indicates the market is bad and our performance may be reduced.', severity="WARNING")
                        return full_conf
                    else:
                        log.info('{0} is too expensive for the max acceptable price: {1}'.format(cluster_conf.instance_type, maximum_accepted_price))
                else:
                    for region, region_conf in regions_conf.items():
                        for az in get_azs_for_region(region_conf):
                            log.info('Working with ondemand instance {0} on region {1} and AZ {2}'.format(cluster_conf.instance_type, region, az))
                            full_conf = FullConf(cluster_conf, region_conf, az)
                            if full_conf not in blacklisted_confs:
                                return full_conf
                            else:
                                log.info("Ignoring this conf because it's blacklisted for now: {0}".format(blacklisted_confs))
            log.info('No cluster configuration is economically viable!')
            inotify('Extremely urgent: All processing is halted!' "\n" 'Time to panic. No cluster configuration is economically viable. Probably the spot market crashed for good. Immediate action is necessary to restore the cluster.')
        except Exception as e:
            log.exception('Exception while getting spot price history')
            inotify('Exception while getting spot price history' "\n" 'This is very weird. If it happens more than once, better panic. Exception is: ' + traceback.format_exc(), severity="WARNING")
        time.sleep(60)

class AssemblyFailedException(Exception): pass

def build_assembly():
    send_heartbeat()
    try:
        cluster.build_assembly()
    except Exception as e:
        log.exception('Failed to build assembly')
        raise AssemblyFailedException()

def killall_jobs(cluster_name, region):
    send_heartbeat()
    try:
        cluster.killall_jobs(cluster_name, region=region)
    except Exception as e:
        pass

def run_sanity_checks(collect_results_dir, full_conf, cluster_name):
    send_heartbeat()
    log.info('Running sanity check')
    region = full_conf.region_conf.region
    cluster_conf = full_conf.cluster_conf
    cluster.health_check(cluster_name=cluster_name, region=region)
    cluster.job_run(cluster_name=cluster_name, job_name=sanity_check_job_name, job_mem=cluster_conf.job_mem,
                    region=region, job_timeout_minutes=sanity_check_job_timeout_minutes,
                    collect_results_dir=collect_results_dir,
                    detached=True, kill_on_failure=True, disable_assembly_build=True)

def destroy_all_clusters(cluster_name):
    send_heartbeat()
    for region in regions_conf:
        cluster.destroy(cluster_name, region=region)

def cluster_job_run(*args, **kwargs):
    send_heartbeat()
    cluster.job_run(*args, **kwargs)

def cluster_destroy(*args, **kwargs):
    send_heartbeat()
    cluster.destroy(*args, **kwargs)

def cluster_launch(*args, **kwargs):
    send_heartbeat()
    cluster.launch(*args, **kwargs)

def save_conf_on_cluster(full_conf, cluster_name):
    send_heartbeat()
    cluster.save_extra_data(object_to_base64(full_conf), cluster_name, region=full_conf.region_conf.region)

def load_conf_from_cluster(cluster_name):
    send_heartbeat()
    for region in regions_conf:
        try:
            data = cluster.load_extra_data(cluster_name, region=region)
            conf = base64_to_object(data)
            log.info('Found conf {0} from existing cluster'.format(conf))
            return conf
        except Exception as e:
            pass
    log.info('No existing cluster conf found')
    return None

def get_ami_for(full_conf):
    if full_conf.cluster_conf.ami_type == 'hvm':
        return full_conf.region_conf.ami_hvm
    else:
        return full_conf.region_conf.ami_pvm

def get_master_ami(full_conf):
    if master_ami_type == 'hvm':
        return full_conf.region_conf.ami_hvm
    else:
        return full_conf.region_conf.ami_pvm

def ensure_cluster(cluster_name, cluster_sequence, full_conf, blacklisted_confs, collect_results_dir, entity_id, disable_vpc, spark_version=spark_version, security_group=default_security_group, tag=[]):
    while True:
        try:
            full_conf = load_conf_from_cluster(cluster_name)
            if not full_conf:
                log.info('No existing cluster found, destroying all possible half-created clusters then proceeding to create a new cluster')
                destroy_all_clusters(cluster_name)
                full_conf = get_best_conf_and_az(blacklisted_confs, cluster_sequence, entity_id=entity_id)
                log.info('Trying to launch cluster with configuration {}'.format(full_conf))
                cluster_launch(cluster_name=cluster_name, slaves=str(full_conf.cluster_conf.slaves),
                               master_instance_type=master_instance_type,
                               instance_type=full_conf.cluster_conf.instance_type,
                               ondemand=full_conf.cluster_conf.spot_price is None,
                               spot_price=str(full_conf.cluster_conf.spot_price),
                               ami=get_ami_for(full_conf),
                               master_ami=get_master_ami(full_conf),
                               worker_instances=str(full_conf.cluster_conf.worker_instances),
                               zone=full_conf.az,
                               vpc = full_conf.region_conf.vpc if not disable_vpc else None,
                               vpc_subnet = full_conf.region_conf.subnet_by_az.get(full_conf.az) if not disable_vpc else None,
                               just_ignore_existing=False,
                               spark_version=spark_version,
                               security_group=security_group, env=env,
                               region=full_conf.region_conf.region, max_clusters_to_create=1,
                               user_data=user_data_script, tag=tag,
                               script_timeout_total_minutes=110,
                               script_timeout_inactivity_minutes=20)
                save_conf_on_cluster(full_conf, cluster_name)
            killall_jobs(cluster_name, full_conf.region_conf.region)
            try:
                run_sanity_checks(collect_results_dir, full_conf, cluster_name)
            except Exception as e:
                log.exception('Sanity check failed')
                notify('Sanity check failed' "\n" 'It will be checked once more before destroying the cluster', severity="WARNING", entity_id=entity_id)
            run_sanity_checks(collect_results_dir, full_conf, cluster_name)
            break
        except Exception as e:
            if full_conf:
                blacklisted_confs.add(full_conf)
            log.exception('Cluster failed')
            notify("""Cluster failed

But don't panic. We will try creating another cluster. See log for more details.
Our current cluster configuration is {0}
Exception is: {1}
"""
                    .format(full_conf, traceback.format_exc()), severity="WARNING", entity_id=entity_id)
            cluster_destroy(cluster_name, region=full_conf.region_conf.region)
    return full_conf


def run_job(cluster_name, job_name, full_conf, collect_results_dir, consecutive_failures=0, entity_id=runner_eid):
    while True:
        try:
            log.info('Running {}'.format(job_name))
            cluster_job_run(cluster_name=cluster_name, job_name=job_name, job_mem=full_conf.cluster_conf.job_mem,
                            region=full_conf.region_conf.region, job_timeout_minutes=(job_timeout_minutes),
                            collect_results_dir=collect_results_dir,
                            detached=True, kill_on_failure=True, disable_assembly_build=True)
            consecutive_failures = 0
            notify('Job execution completed successfully :)', severity="RESOLVE", entity_id=entity_id)
            return (True, 0)
        except Exception as e_job:
            consecutive_failures += 1
            log.exception('Job execution failed')
            notify("""Job execution failed

But don't panic. We will try again. See log for more details.
We had {0} consecutive failures of maximum of {1}.
Exception is: {2}
"""
                   .format(consecutive_failures, max_errors_on_healthy_cluster, traceback.format_exc()), severity="WARNING", entity_id=entity_id)
            try:
                run_sanity_checks(collect_results_dir, full_conf, cluster_name)
                if consecutive_failures >= max_errors_on_healthy_cluster:
                    log.error('Max number of consecutive failures reached. Killing healthy cluster =(')
                    notify("Killing healthy cluster =(" "\n\n"
                           "Max number of consecutive failures reached. Something is very very wrong. As always, we will try again, but better check those logs.",
                           severity="CRITICAL", entity_id=entity_id)
                    cluster_destroy(cluster_name, region=full_conf.region_conf.region)
                    return (False, 0)
            except Exception as e_sanity:
                log.exception('Sanity check failed')
                notify("Sanity check failed\nThe cluster was healthy but now it looks unhealthy. More checks will come soon.", severity="WARNING", entity_id=entity_id)
                return (False, consecutive_failures)
    

def setup(job_name):
    setup_eid = "{0}-{1}".format(job_name, runner_eid)
    notify("Initializing job runner\nIt's a pleasure to be back.", severity="INFO", entity_id=setup_eid)
    while True:
        try:
            if cluster.get_assembly_path() is None:
                build_assembly()
            break
        except AssemblyFailedException as e:
            notify('Failed to build assembly' + "\n" +
                   'Someone messed up with the installation/deploy. An urgent action is needed. Exception is: ' + traceback.format_exc(),
                   entity_id=setup_eid)


from argh import *


def run_continuously(collect_results_dir, namespace_, job_name, cluster_name_prefix, cluster_sequence,
                     disable_vpc=False, security_group=default_security_group, tag=[]):
    global namespace
    namespace = namespace_

    setup(job_name)

    consecutive_failures = 0

    blacklisted_confs = ExpireCollection()
    cluster_name = "{0}-{1}-{2}".format(cluster_name_prefix,
                                        "classic" if disable_vpc else "vpc",
                                        env)

    full_conf = None
    entity_id = "{0}-runner-{1}".format(cluster_name_prefix, uuid.uuid1())
    
    def inotify(message, severity="CRITICAL"):
        return notify(message, severity, entity_id=entity_id)
    while True: 
        try:
            full_conf = ensure_cluster(cluster_name, cluster_sequence, full_conf, 
                                       blacklisted_confs, collect_results_dir, entity_id, disable_vpc, security_group=security_group, tag=tag)
            while True:
                success, consecutive_failures = run_job(cluster_name, job_name, full_conf, collect_results_dir, 
                                                        consecutive_failures, entity_id)
                if not success:
                    break
        except Exception as e:
            log.exception('Completely unknown exception')
            inotify("Completely unknown exception\nTime to panic. Exception is: " + traceback.format_exc())


def run_once(collect_results_dir, namespace_, job_name, cluster_name_prefix, cluster_sequence,
             disable_vpc=False, security_group=default_security_group, tag=[]):
    
    global namespace
    namespace = namespace_

    setup(job_name)

    consecutive_failures = 0

    blacklisted_confs = ExpireCollection()
    cluster_name = "{0}-{1}-{2}".format(cluster_name_prefix,
                                        "classic" if disable_vpc else "vpc",
                                        env)

    full_conf = None
    entity_id = "{0}-runner-{1}".format(cluster_name_prefix, uuid.uuid1())
    def inotify(message, severity="CRITICAL"):
        return notify(message, severity, entity_id=entity_id)

    while True:
        try:
            full_conf = ensure_cluster(cluster_name, cluster_sequence, full_conf, 
                                       blacklisted_confs, collect_results_dir, entity_id, disable_vpc, security_group=security_group, tag=tag)
            success, consecutive_failures = run_job(cluster_name, job_name, full_conf, collect_results_dir, 
                                                    consecutive_failures, entity_id)
            if success:
                break
        except Exception as e:
            log.exception('Completely unknown exception')
            inotify("Completely unknown exception\nTime to panic. Exception is: " + traceback.format_exc())
    destroy_all_clusters(cluster_name)


def mysetup1(collect_results_dir, disable_vpc = False, security_group = default_security_group):
    # This job will run once, then the cluster will destroyed
    run_once(collect_results_dir,
            "app1",
             job_name="MyFirstSetup",
             cluster_name_prefix="mysetup1",
             cluster_sequence=mysetup1_cluster_sequence,
             disable_vpc=disable_vpc,
             security_group=security_group,
             tag=["tag1=value1"])

def mysetup2(collect_results_dir, disable_vpc = False, security_group = default_security_group):
    # This job will run a in loop, forever
    run_continuously(collect_results_dir,
            "app2",
             job_name="MySecondSetup",
             cluster_name_prefix="mysetup2",
             cluster_sequence=mysetup1_cluster_sequence,
             disable_vpc=disable_vpc,
             security_group=security_group,
             tag=["tag1=value1"])
    
class ExpireCollection:
    """
    >>> c = ExpireCollection(timeout=0.5)
    >>> import time
    >>> c.add('something')
    >>> len(c)
    1
    >>> 'something' in c
    True
    >>> time.sleep(0.6)
    >>> len(c)
    0
    """
    def __init__(self, timeout=60*60*3):
        import collections
        self.timeout = timeout
        self.events = collections.deque()

    def add(self, item):
        import threading
        self.events.append(item)
        threading.Timer(self.timeout, self.expire).start()

    def __len__(self):
        return len(self.events)

    def expire(self):
        """Remove any expired events
        """
        self.events.popleft()

    def __str__(self):
        return str(self.events)

    def __contains__(self, elem):
        return elem in self.events


def object_to_base64(obj):
    return base64.b64encode(pickle.dumps(obj))

def base64_to_object(b64):
    return pickle.loads(base64.b64decode(b64))

if __name__ == '__main__':
    import doctest
    doctest.testmod()
    parser = ArghParser()
    # All setups must be added here
    parser.add_commands([mysetup1, mysetup2])
    parser.dispatch()
