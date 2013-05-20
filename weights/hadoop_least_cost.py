from nova.openstack.common import log as logging
from nova.compute import api as compute
from nova.compute import vm_states

METADATA_APPLICATION_KEY = "application"
METADATA_APPLICATION_VALUE_HADOOP = "hadoop"

METADATA_HADOOP_KEY = "hadoop"
METADATA_HADOOP_VALUE_NAMENODE = "namenode"
METADATA_HADOOP_VALUE_DATANODE = "datanode"
METADATA_HADOOP_VALUE_JOBTRACKER = "jobtracker"
METADATA_HADOOP_VALUE_TASKTRACKER = "tasktracker"

METADATA_HADOOP_DEPLOYMENT_KEY = "hadoop_deployment"


LOG = logging.getLogger(__name__)

COMPUTE_API = compute.API()


def hadoop_cost_fn(host_state, weighing_properties):

    """
    This function accounts both the benefit coming from
    datanodes and tasktrackers co-location on the host
    (if they belongs to the same Hadoop deployment of
    the instance to be scheduled).

    CONFIGURATIONS: in nova.conf
    least_cost_functions=nova.scheduler.least_cost.compute_fill_first_cost_fn,nova.scheduler.hadoop_least_cost.hadoop_cost_fn
    compute_fill_first_cost_fn_weight=-1.0
    hadoop_cost_fn_weight=-1024

    IMPORTANT NOTE: this function works only if an instance
    at the time is scheduled. See documentation for further info.

    Note: The two terms are placed here together for
    performance reasons, so their weights are hardcoded.
    However, a common corrective weight should be specified
    in nova.conf.
    See documentation for additional info
    """

    DATANODE_WEIGHT = 2.0
    TASKTRACKER_WEIGHT = 1.0

    me = host_state.host
    cost = 0

    context = weighing_properties['context']
    # Retrieves image metadata from the request_spec.instance_properties part of the RPC message
    # WARNING: Differs from what is written in the IBM paper
    spec = weighing_properties.get('request_spec', {})
    image_metadata = spec.get('instance_properties', {}).get('metadata', {})
    if (image_metadata
        and image_metadata.get(METADATA_APPLICATION_KEY) == METADATA_APPLICATION_VALUE_HADOOP
        and image_metadata.get(METADATA_HADOOP_DEPLOYMENT_KEY)):

        #TODO: passing some search_opts to COMPUTE_API.get_all it should be possible to query instances
        #both per host and per metadata, and so to get directly common_intances

        # search_opts = {
        #     'metadata': [
        #         {'%s' % (METADATA_APPLICATION_KEY): '%s' % (METADATA_APPLICATION_VALUE_HADOOP)},
        #         {'%s' % (METADATA_HADOOP_DEPLOYMENT_KEY): '%s' % (image_metadata.get(METADATA_HADOOP_DEPLOYMENT_KEY))}
        #     ],
        #     'host': '%s' % (me),
        #     'vm_state': '%s|%s' % (vm_states.ACTIVE, vm_states.BUILDING)
        #     }
        # common_intances = COMPUTE_API.get_all(context, search_opts=search_opts)

        #Select all the Hadoop instances running on the host
        instances = _all_host_application_instances(context, me, METADATA_APPLICATION_VALUE_HADOOP)
        #Check if instances are not deleted (the previous returns deleted instances) and running or building
        instances = [inst for inst in instances
            if (inst['vm_state'] == vm_states.ACTIVE or inst['vm_state'] == vm_states.BUILDING)]

        if instances:
            LOG.debug(_("Hadoop instances found on host %(me)"),
                        locals())

            #Select the instances from the same deployment
            common_instances = [uiid for uiid in instances.keys()
                if COMPUTE_API.get_instance_metadata(context, instances[uiid]).get(METADATA_HADOOP_DEPLOYMENT_KEY)
                == image_metadata.get(METADATA_HADOOP_DEPLOYMENT_KEY)]
            if common_instances:
                LOG.debug(_("Hadoop instances of the same deployment",
                "%(image_metadata.get(METADATA_HADOOP_DEPLOYMENT_KEY)) found"),
                        locals())

                # Selects the datanode instances within the instances
                datanodes = [uiid for uiid in common_instances.keys()
                    if COMPUTE_API.get_instance_metadata(context, common_instances[uiid]).get(METADATA_HADOOP_KEY)
                    == METADATA_HADOOP_VALUE_DATANODE]
                if datanodes:
                    LOG.debug(_("%(len(datanodes)) datanode instances found"),
                        locals())

                    cost += DATANODE_WEIGHT * len(datanodes)

                # Selects the tasktracker instances within the instances
                tasktrackers = [uiid for uiid in common_instances.keys()
                    if COMPUTE_API.get_instance_metadata(context, common_instances[uiid]).get(METADATA_HADOOP_KEY)
                    == METADATA_HADOOP_VALUE_TASKTRACKER]
                if tasktrackers:
                    LOG.debug(_("%(len(tasktrackers)) datanode instances found"),
                        locals())

                    cost += TASKTRACKER_WEIGHT * len(tasktrackers)
    return cost


def _all_host_application_instances(self, context, host, application):
    """
    Return all the instances of a given application on a certain host.
    All instances in the system if context is admin
    Key = instance_uuid, Value = instance
    """

    all_instances = {}
    # Retrieves all the instances given a certain context
    # TODO: Look if it returns deleted instances as well
    for instance in COMPUTE_API.get_all(context):
        if instance['host'] == host:
            # Retrieves metadata of an instance
            metadata = COMPUTE_API.get_instance_metadata(context, instance)
            # Looks for the application metadata
            if METADATA_APPLICATION_KEY in metadata:
                # Stores the application metadata value
                instance_application = metadata[METADATA_APPLICATION_KEY]
                if instance_application == application:
                    # Stores the entry (instance uuid, instance reference)
                    all_instances[instance['uuid']] = instance
                    LOG.debug(_("Instance %(instance['uuid'])contains "
                        "the application metadata %(application) on host %(host)"),
                        locals())
        return all_instances


# NOTES
#scheduler_hints = filter_properties.get('scheduler_hints') or {}
#
#image_props = spec.get('image', {}).get('properties', {})
#
#def get_all(self, context, search_opts=None, sort_key='created_at',
#               sort_dir='desc', limit=None, marker=None):
#"""Get all instances filtered by one of the given parameters.
#
#If there is no filter and the context is an admin, it will retrieve
#all instances in the system.
#
#Deleted instances will be returned by default, unless there is a
#search option that says otherwise.
#
#The results will be returned sorted in the order specified by the
#'sort_dir' parameter using the key specified in the 'sort_key'
#parameter.
#"""
