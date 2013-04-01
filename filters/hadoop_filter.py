from nova.scheduler import filters
from nova.compute import api as compute


class ApplicationAwareFilter(filters.BaseHostFilter):
    """Base class for all application aware filters. Retrieves instances information"""

    METADATA_APPLICATION_KEY = "application"

    def __init__(self):
        self.compute_api = compute.API()

    def _all_hosts(self, context, application):
        """Return a dictionary with all the host where an instance of the given application is running.
        Key = instance_uuid, Value = host"""

        all_hosts = {}
        for instance in self.compute_api.get_all(context):
            metadata = self.compute_api.get_instance_metadata(context, instance)
            if ApplicationAwareFilter.METADATA_APPLICATION_KEY in metadata:
                instance_application = metadata[ApplicationAwareFilter.METADATA_APPLICATION_KEY]
                if instance_application == application:
                    all_hosts[instance['uuid']] = instance['host']
        return all_hosts

    def _all_instances(self, context, application):
        """Return all the instances of a given application. All instances in the system if context is admin
        Key = instance_uuid, Value = instance"""
        all_instances = {}
        for instance in self.compute_api.get_all(context):
            metadata = self.compute_api.get_instance_metadata(context, instance)
            if ApplicationAwareFilter.METADATA_APPLICATION_KEY in metadata:
                instance_application = metadata[ApplicationAwareFilter.METADATA_APPLICATION_KEY]
                if instance_application == application:
                    all_instances[instance['uuid']] = instance


class HadoopFilter(ApplicationAwareFilter):
    """Hadoop aware filter. Return the host where a DataNode is running"""

    METADATA_APPLICATION_VALUE_HADOOP = "hadoop"
    METADATA_HADOOP_KEY = "hadoop"
    METADATA_HADOOP_VALUE_NAMENODE = "namenode"
    METADATA_HADOOP_DATANODE_VALUE = "datanode"
    METADATA_HADOOP_JOBTRACKER_VALUE = "jobtracker"
    METADATA_HADOOP_TASKTRACKER_VALUE = "tasktracker"

    def host_passes(self, host_state, filter_properties):
        context = filter_properties['context']
        #scheduler_hints = filter_properties.get('scheduler_hints') or {}

        spec = filter_properties.get('request_spec', {})
        image_props = spec.get('image', {}).get('properties', {})
        image_props = spec.get('instance_properties', {}).get('metadata', {})


        me = host_state.host

        affinity_uuids = scheduler_hints.get('different_host', [])
        if isinstance(affinity_uuids, basestring):
            affinity_uuids = [affinity_uuids]
        if affinity_uuids:
            all_hosts = self._all_hosts(context)
            return any([i for i in affinity_uuids if all_hosts.get(i) == me])
        # With no different_host key
        return True


 #def get_all(self, context, search_opts=None, sort_key='created_at',
 #               sort_dir='desc', limit=None, marker=None):
"""Get all instances filtered by one of the given parameters.

If there is no filter and the context is an admin, it will retrieve
all instances in the system.

Deleted instances will be returned by default, unless there is a
search option that says otherwise.

The results will be returned sorted in the order specified by the
'sort_dir' parameter using the key specified in the 'sort_key'
parameter.
"""
