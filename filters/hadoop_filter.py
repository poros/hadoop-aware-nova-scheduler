from nova.scheduler import filters
from nova.compute import api as compute
from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class ApplicationAwareFilter(filters.BaseHostFilter):
    """Base class for all application aware filters. Retrieves instances information"""
    # Inherit from the base filter class

    # The application type is assumed to be saved in the instance metadata
    METADATA_APPLICATION_KEY = "application"

    # Stores a reference to the compute API
    def __init__(self):
        self.compute_api = compute.API()

    def _all_hosts(self, context):
        """Return a dictionary with all the host.
            Key = instance_uuid, Value = host"""
        all_hosts = {}
        for instance in self.compute_api.get_all(context):
            all_hosts[instance['uuid']] = instance['host']
        return all_hosts

    def _all_application_hosts(self, context, application):
        """Return a dictionary with all the host where an instance of the given application is running.
        Key = instance_uuid, Value = host"""

        all_hosts = {}
        # Retrieves all the instances given a certain context
        # TODO: Look if it returns deleted instances as well
        for instance in self.compute_api.get_all(context):
            # Retrieves metadata of an instance
            metadata = self.compute_api.get_instance_metadata(context, instance)
            # Looks for the application metadata
            if ApplicationAwareFilter.METADATA_APPLICATION_KEY in metadata:
                # Stores the application metadata value
                instance_application = metadata[ApplicationAwareFilter.METADATA_APPLICATION_KEY]
                if instance_application == application:
                    # Stores the entry (instance uuid, host)
                    all_hosts[instance['uuid']] = instance['host']
                    LOG.debug(_("Instance %(instance['uuid'])contains "
                        "the application metadata"),
                        locals())
        return all_hosts

    def _all_application_instances(self, context, application):
        """Return all the instances of a given application. All instances in the system if context is admin
        Key = instance_uuid, Value = instance"""

        all_instances = {}
        # Retrieves all the instances given a certain context
        # TODO: Look if it returns deleted instances as well
        for instance in self.compute_api.get_all(context):
            # Retrieves metadata of an instance
            metadata = self.compute_api.get_instance_metadata(context, instance)
            # Looks for the application metadata
            if ApplicationAwareFilter.METADATA_APPLICATION_KEY in metadata:
                # Stores the application metadata value
                instance_application = metadata[ApplicationAwareFilter.METADATA_APPLICATION_KEY]
                if instance_application == application:
                    # Stores the entry (instance uuid, instance reference)
                    all_instances[instance['uuid']] = instance
                    LOG.debug(_("Instance %(instance['uuid'])contains "
                        "the application metadata"),
                        locals())
        return all_instances


class HadoopFilter(ApplicationAwareFilter):
    """Hadoop aware filter. Schedule the instance on an host where a datanode is running"""
    # Inherit from the base Application Aware Filter class

    # Constant string values for Hadoop
    METADATA_APPLICATION_VALUE_HADOOP = "hadoop"
    METADATA_HADOOP_KEY = "hadoop"
    METADATA_HADOOP_VALUE_NAMENODE = "namenode"
    METADATA_HADOOP_VALUE_DATANODE = "datanode"
    METADATA_HADOOP_VALUE_JOBTRACKER = "jobtracker"
    METADATA_HADOOP_VALUE_TASKTRACKER = "tasktracker"

    # Method that every filter has to implement
    # Return True if the host can be included in the candidates list, False otherwise
    def host_passes(self, host_state, filter_properties):
        # Retrieves context from the the filter_properties part of the RPC message
        # WARNING: Differs from what is written in the IBM paper
        context = filter_properties['context']
        # Retrieves image metadata from the request_spec.instance_properties part of the RPC message
        # WARNING: Differs from what is written in the IBM paper
        spec = filter_properties.get('request_spec', {})
        image_metadata = spec.get('instance_properties', {}).get('metadata', {})
        if image_metadata:
            # Retrieves all Hadoop instances
            # TODO: check if it already discriminate instances by user thanks to context
            instances = self._all_application_instances(context, HadoopFilter.METADATA_APPLICATION_VALUE_HADOOP)
            if instances:
                LOG.debug(_("Hadoop instances found"),
                        locals())
                # Selects the datanode instances within all the Hadoop instances
                datanodes = [uiid for uiid in instances.keys()
                    if self.get_instance_metadata(context, instances[uiid]).get[HadoopFilter.METADATA_HADOOP_KEY]
                    == HadoopFilter.METADATA_HADOOP_VALUE_DATANODE]
                if datanodes:
                    LOG.debug(_("Datanode instances found"),
                        locals())
                    # Retrieves all the hosts
                    hosts = self._all_hosts(context)
                    if hosts:
                        # Get the current host
                        me = host_state.host
                        # Return if the current host match any of the hosts running a datanode application instance
                        return any([uiid for uiid in datanodes if hosts.get(uiid) == me])
        return True


# NOTES
#scheduler_hints = filter_properties.get('scheduler_hints') or {}
#image_props = spec.get('image', {}).get('properties', {})
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
