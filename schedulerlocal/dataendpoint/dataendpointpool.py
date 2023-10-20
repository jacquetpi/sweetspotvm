from schedulerlocal.dataendpoint.dataendpoint import DataEndpoint
from schedulerlocal.node.jsonencoder import GlobalEncoder
import json

class DataEndpointPool(object):
    """
    An EndpointPool is a class composed of a loading endpoint and a saving endpoint
    ...

    Public Methods
    -------
    load()
        Return data from the loader, while also storing to the saver if it is defined
    loadOnly()
        Return data from the loader
    """

    def __init__(self, **kwargs):
        req_attributes = ['loader','saver']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])

    def load_subset(self, timestamp, subset):
        """Return subset data (subset usage and vm usage) from the loader, while also storing to the saver if it is defined
        ----------

        Parameters
        ----------
        timestamp : int 
            timestamp requested
        subset : Subset
            Subset Object

        Return
        ----------
        data : dict
            Data as dict
        """
        subset_usage, vm_usage_dict = self.load_subset_only(timestamp, subset)
        if self.saver != None:
            # Subset record
            self.saver.store(DataEndpoint.record(tmp=timestamp, rec='subset',\
                res=subset.get_res_name(), val=subset_usage, config=subset.get_capacity(),\
                subset='subset-' + str(subset.get_oversubscription_id()),\
                sb_oc=str(subset.get_oversubscription_id()),\
                sb_unused=subset.unused_resources_count(),\
                sb_dsc=json.dumps(subset, cls=GlobalEncoder)))
            # VM records
            for vm_uuid, vm_tuple in vm_usage_dict.items():
                vm_object, vm_usage = vm_tuple
                self.saver.store(DataEndpoint.record(tmp=timestamp, rec='vm',\
                    res=subset.get_res_name(), val=vm_usage, config=subset.get_vm_allocation(vm_object),\
                    subset='subset-' + str(subset.get_oversubscription_id()),\
                    sb_oc=subset.get_oversubscription_id(),\
                    vm_uuid=vm_uuid,\
                    vm_cmn=vm_object.get_name()))
        return subset_usage, vm_usage_dict

    def load_subset_only(self, timestamp, subset):
        """Return subset data from the loader
        ----------

        Parameters
        ----------
        timestamp : int 
            timestamp requested
        SubsetManager : Subset
            SubsetManager Object

        Return
        ----------
        data : dict
            Data as dict
        """
        return self.loader.load_subset(timestamp, subset)

    def load_global(self, timestamp, subset_manager):
        """Return global data from the loader, while also storing to the saver if it is defined
        ----------

        Parameters
        ----------
        timestamp : int 
            timestamp requested
        subset_manager : SubsetManager
            SubsetManager Object

        Return
        ----------
        data : dict
            Data as dict
        """
        data = self.load_global_only(timestamp, subset_manager)
        if self.saver != None:
            self.saver.store(DataEndpoint.record(tmp=timestamp, rec='global',\
                res=subset_manager.get_res_name(), val=data, config=subset_manager.get_capacity()))
        return data

    def load_global_only(self, timestamp, subset_manager):
        """Return subset data from the loader
        ----------

        Parameters
        ----------
        timestamp : int 
            timestamp requested
        subset_manager : SubsetManager
            SubsetManager Object

        Return
        ----------
        data : dict
            Data as dict
        """
        return self.loader.load_global(timestamp, subset_manager)

    def is_live(self):
        """Return a boolean value based on if values are loaded from a live system
        ----------

        Return
        ----------
        Live : boolean
            True if system is live
        """
        return self.loader.is_live()

    def get_timestamp_list(self):
        """Return list of timestamp from loader object. Intended to be used only on an offline setting
        ----------

        Return
        ----------
        timestamps : List
            List of timestamp
        """
        return self.loader.get_timestamp_list()

    def get_deployed_vm_on(self, timestamp):
        """Return deployed vm on given timestamp from loader object. Intended to be used only on an offline setting
        ----------
        """
        return self.loader.get_deployed_vm_on(timestamp)

    def get_destroyed_vm_on(self, timestamp):
        """Return destroyed vm on given timestamp from loader object. Intended to be used only on an offline setting
        ----------
        """
        return self.loader.get_destroyed_vm_on(timestamp)