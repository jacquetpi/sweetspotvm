from schedulerlocal.subset.subsetoversubscription import SubsetOversubscription, SubsetOversubscriptionStatic
from schedulerlocal.domain.domainentity import DomainEntity
from schedulerlocal.domain.libvirtconnector import LibvirtConnector, ConsumerNotAlived
from schedulerlocal.dataendpoint.dataendpointpool import DataEndpointPool
from schedulerlocal.predictor.predictor import PredictorCsoaa
import os, numpy as np
from math import ceil

class Subset(object):
    """
    A Subset is an arbitrary group of physical resources to which consumers (e.g. VMs) can be attributed
    ...

    Attributes
    ----------
    res_list : list
        List of physical resources
    consumer_list : list
        List of consumers

    Public Methods
    -------
    add_res()
        Add a resource to subset
    remove_res()
        Remove a resource from subset
    get_res()
        Get resources list
    count_res()
        Count resources in subset
    add_consumer()
        Add a consumer to subset
    remove_consumer()
        Remove a consumer from subset
    count_consumer()
        Count resources in subset
    """
    def __init__(self, **kwargs):
        self.oversubscription = SubsetOversubscriptionStatic(subset=self, ratio=kwargs['oversubscription'])
        self.endpoint_pool = kwargs['endpoint_pool']
        opt_attributes = ['res_list', 'consumer_list']
        for opt_attribute in opt_attributes:
            opt_val = kwargs[opt_attribute] if opt_attribute in kwargs else list()
            setattr(self, opt_attribute, opt_val)

    def get_oversubscription_id(self):
        """Get subset id
        ----------

        Return
        ----------
        id : float
            Oversubscription as ID
        """
        return self.oversubscription.get_id()

    def add_res(self, res):
        """Add a resource to subset
        ----------

        Parameters
        ----------
        res : object
            The resource to add
        """
        if res in self.res_list: raise ValueError('Cannot add twice a resource', res)
        self.res_list.append(res)

    def remove_res(self, res):
        """Remove a resource to subset
        ----------

        Parameters
        ----------
        res : object
            The resource to remove
        """
        self.res_list.remove(res)

    def get_res(self):
        """Get resources list
        ----------

        Return
        ----------
        res : list
            resources list
        """
        return self.res_list

    def has_vm(self, vm : DomainEntity):
        """Test if a vm is present in subset
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        success : bool
            Return success status of operation
        """
        warning_message = 'Warning: consumer found in subset ' + self.get_res_name() + '-' + str(self.get_oversubscription_id()) + 'while being destroyed:'
        for consumer in self.consumer_list:
            # Search by uuid if available
            if (consumer.get_uuid() == None) and (consumer.get_name() == vm.get_name()): 
                if consumer.is_being_destroyed():
                    print(warning_message + ' ' + consumer.get_name() + ' ' + consumer.get_uuid())
                    return False
                return True
            # Otherwise search by name
            elif (consumer.get_uuid() != None) and (consumer.get_uuid() == vm.get_uuid()):
                if consumer.is_being_destroyed():
                    print(warning_message + ' ' + consumer.get_name() + ' ' + consumer.get_uuid())
                    return False
                return True
        return False

    def get_vm_by_name(self, name : str):
        """Get a vm by its name, none if not present
        ----------

        Parameters
        ----------
        name : str
            The name to search for

        Returns
        -------
        vm : DomainEntity
            None if not present
        """
        for consumer in self.consumer_list:
            if consumer.get_name() == name: return consumer
        return None

    def get_res_name(self):
        """Get resource name managed by susbset. Resource dependant. Must be reimplemented
        ----------

        Return
        ----------
        res : str
            resource name
        """
        raise NotImplementedError()

    def count_res(self):
        """Count resources in subset
        ----------

        Returns
        -------
        count : int
            number of resources
        """
        return len(self.res_list)

    def add_consumer(self, consumer):
        """Add a consumer to subset. Should not be called directly. Use deploy() instead
        ----------

        Parameters
        ----------
        consumer : object
            The consumer to add
        """
        if consumer in self.consumer_list: raise ValueError('Cannot add twice a consumer', consumer)
        self.consumer_list.append(consumer)

    def remove_consumer(self, consumer):
        """Remove a consumer from subset
        ----------

        Parameters
        ----------
        consumer : object
            The consumer to remove
        """
        if consumer not in self.consumer_list:
            if consumer == None: print('Warning: trying to remove a null consumer')
            else: print('Warning: trying to remove a non present consumer', consumer.get_name())
            return
        self.consumer_list.remove(consumer)

    def count_consumer(self):
        """Count consumers in subset
        ----------

        Returns
        -------
        count : int
            number of consumers
        """
        return len(self.consumer_list)

    def get_consumers(self):
        """Get consumers list
        ----------

        Return
        ----------
        consumers : list
            consumers list
        """
        return self.consumer_list

    def get_additional_res_count_required_for_vm(self, vm : DomainEntity):
        """Return the number of additional resource required to deploy specified vm. 0 if no additional resources is required
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        missing : int
            number of missing physical resources
        """
        return self.oversubscription.get_additional_res_count_required_for_vm(vm)

    def unused_resources_count(self):
        """Return the number of resource unused
        ----------

        Returns
        -------
        unused : int
            count of unused resources
        """
        return self.oversubscription.unused_resources_count()

    def get_allocation(self):
        """Return allocation of subset (number of resources requested without oversubscription consideration)
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        allocation : int
            Sum of resources requested
        """
        allocation = 0
        for consumer in self.consumer_list: allocation+= self.get_vm_allocation(consumer)
        return allocation

    def get_vm_allocation(self, vm : DomainEntity):
        """Return allocation of a given VM. Resource dependant. Must be reimplemented
        Allocation : number of resources requested, without oversubscription consideration
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        allocation : int
            Number of resources requested by the VM
        """
        raise NotImplementedError()

    def get_max_consumer_allocation(self):
        """Return the highest allocation between consumers
        Allocation : number of resources requested, without oversubscription consideration
        ----------

        Returns
        -------
        allocation : int
            Number of resources requested by the VM
        """
        max_allocation = 0
        for consumer in self.consumer_list: 
            if max_allocation < self.get_vm_allocation(consumer): max_allocation = self.get_vm_allocation(consumer)
        return max_allocation

    def get_capacity(self):
        """Return subset physical resource capacity. Resource dependant. Must be reimplemented
        Capacity : number of resources which can be used by VM

        Returns
        -------
        capacity : int
            Subset resource capacity
        """
        raise NotImplementedError()

    def get_usage(self):
        """Get history of usage on resources from endpoint

        Returns
        -------
        Usage : dict
            data as dict
        """
        return self.endpoint_pool.load_subset(subset=self) # TODO

    def get_oversubscription(self):
        """Getter on oversubscription computation

        Returns
        -------
        computation : SubsetOversubscription
            subset oversubscription computation
        """
        return self.oversubscription

    def get_current_resources_usage(self):
        """Get current usage of physical resources. Resource dependant. Must be reimplemented

        Returns
        -------
        usage : int
            Percentage [0:1]
        """
        raise NotImplementedError()

    def get_current_consumers_usage(self):
        """Get current CPU usage of consumers

        Returns
        -------
        usage : dict
            dict of consumer id with their Percentage [0:1]
        """
        usage = dict()
        for consumer in self.consumer_list:
            if not consumer.is_deployed(): continue
            try:
                data = self.get_current_consumer_usage(consumer)
            except ConsumerNotAlived as ex: continue
            usage[consumer.get_uuid()] = (consumer, data) # due to serialization needs on key
        return usage

    def get_current_consumer_usage(self, consumer : DomainEntity):
        """Get current usage of a single consumer. Resource dependant. Must be reimplemented

        Parameters
        ----------
        consumer : DomainEntity
            The VM to consider

        Returns
        -------
        usage : float
            Percentage [0:1]
        """
        raise NotImplementedError()

    def deploy(self, vm : DomainEntity):
        """Deploy a VM on resources. Resource dependant. Must be reimplemented with a super call
        Should adapt the DomainEntity object as required before the subsetManager applies changes with connector
        """
        available_oversubscribed = self.oversubscription.get_available(with_new_vm=True)
        if available_oversubscribed < self.get_vm_allocation(vm): 
            print('Warning: Not enough resources available to deploy', vm.get_name(), 'on res', self.get_res_name(), 'for request', self.get_vm_allocation(vm))
            return False
        self.add_consumer(vm)
        return True

    def status(self):
        """Build and return a dict representating current allocation and available oversubscribed quantities
        ----------

        Returns
        ----------
        status : dict
            Subset status as dict
        """
        return {'pcap': self.get_capacity(), 'palloc': self.get_allocation(), 'vavail': self.oversubscription.get_available(with_new_vm=True)}

    def update_monitoring(self, timestamp : int):
        """Order a monitoring session on current subset with specified timestamp key
        Use endpoint_pool to load and store from the appropriate location
        ----------

        Parameters
        ----------
        timestamp : int
            The timestamp key

        Returns
        -------
        current_usage : float
            resource usage percentage
        consumers_usage : dict
            consumer resource usage percentage
        clean_needed : bool
            If VM left out of the scope of the scheduler (without passing by manager), return True
        """
        subset_usage, consumers_usage = self.endpoint_pool.load_subset(timestamp=timestamp, subset=self)
        clean_needed = False
        for consumer in self.consumer_list: # Update consumer list
            if consumer.get_uuid() not in consumers_usage.keys():
                if consumer.is_deployed():
                    print('Warning: a VM left without passing by scheduler', consumer.get_name())
                    self.remove_consumer(consumer)
                    clean_needed = True
        return subset_usage, consumers_usage, clean_needed

class SubsetCollection(object):
    """
    A SubsetCollection is a collection of Subset
    ...

    Attributes
    ----------
    subset_dict : list
        List of subsets

    Public Methods
    -------
    add_subset()
        Add a subset
    remove_subset()
        Remove a resource from subset
    count_subset()
        Count resources in subset
    """

    def __init__(self, **kwargs):
        self.subset_dict = dict()

    def add_subset(self, id : float, subset : Subset):
        """Add a subset to collection
        ----------

        Parameters
        ----------
        res : Subset
            The subset to add
        """
        if id in self.subset_dict: raise ValueError('Subset id already exists')
        self.subset_dict[id] = subset

    def remove_subset(self, id : float):
        """Remove a subset from collection
        ----------

        Parameters
        ----------
        id : str
            The subset id to remove
        """
        if id in self.subset_dict: del self.subset_dict[id]

    def get_subset(self, id : float):
        """Get a subset from collection
        ----------

        Parameters
        ----------
        res : Subset
            The subset id to get
        """
        if id not in self.subset_dict: raise ValueError('Subset id does not exist')
        return self.subset_dict[id]

    def get_subsets(self):
        """Get subsets list
        ----------
        """
        return self.subset_dict.values()

    def get_dict(self):
        """Get subsets as dict
        ----------
        """
        return self.subset_dict

    def contains_subset(self, id : float):
        """Check if specified subset id exists
        ----------

        Parameters
        ----------
        res : bool
            Subset presence
        """
        return (id in self.subset_dict)

    def count_subset(self):
        """Count subset in collection
        ----------

        Returns
        -------
        count : int
            number of consumers
        """
        return len(self.subset_dict)

    def get_capacity(self):
        """Return the capacity sum of all subsets
        ----------

        Returns
        -------
        capacity : int
            Overall subset capacity
        """
        capacity = 0
        for subset in self.subset_dict.values(): capacity += subset.get_capacity()
        return capacity

    def get_res(self):
        """Return a list of all subset resources concatened
        ----------

        Returns
        -------
        res : list
            subset resources concatened
        """
        res = list()
        for subset in self.subset_dict.values(): res.extend(subset.get_res())
        return res

    def has_vm(self, vm : DomainEntity):
        """Test if a VM is present in a subset
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        success : bool
            Return success status of operation
        """
        for subset in self.subset_dict.values(): 
            if subset.has_vm(vm): return True
        return False

    def get_vm_by_name(self, name : str):
        """Get a vm by its name, none if not present
        ----------

        Parameters
        ----------
        name : str
            The name to search for

        Returns
        -------
        vm : DomainEntity
            None if not present
        """
        for subset in self.subset_dict.values():
            vm = subset.get_vm_by_name(name)
            if vm != None: return vm
        return None

    def update_monitoring(self, timestamp : int):
        """Order a monitoring session on each subset with specified timestamp key
        Use endpoint_pool to load and store from the appropriate location
        ----------

        Parameters
        ----------
        timestamp : int
            The timestamp key

        Returns
        -------
        clean_needed_list : list()
            List of subset having VM which left without passing by our scheduler methods
        """
        clean_needed_list = list()
        for subset in self.subset_dict.values():
            __, __, clean_needed = subset.update_monitoring(timestamp=timestamp) 
            if clean_needed: clean_needed_list.append(subset)
        return clean_needed_list

    def get_consumers(self):
        """Get List of hosted VMs
        ----------

        Return
        ----------
        vm : list
            List of hosted vm
        """
        consumers = list()
        for subset in self.subset_dict.values(): consumers.extend([vm.get_name() for vm in subset.get_consumers()])
        return consumers

    def __str__(self):
        return ''.join(['|_>' + str(v) + '\n' for v in self.subset_dict.values()])

class CpuSubset(Subset):
    """
    A CpuSubset is an arbitrary group of physical CPU to which consumers (e.g. VMs) can be attributed
    ...

    Public Methods reimplemented/introduced
    -------
    todo()
        todo
    """

    def __init__(self, **kwargs):
        additional_attributes = ['connector', 'cpu_explorer', 'cpu_count', 'offline']
        for req_attribute in additional_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', additional_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        super().__init__(**kwargs)

    def get_res_name(self):
        """Get resource name managed by susbset
        ----------

        Return
        ----------
        res : str
            resource name
        """
        return 'cpu'

    def get_vm_allocation(self, vm : DomainEntity):
        """Return CPU allocation of a given VM without oversubscription consideration
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        allocation : int
            Number of resources requested by the VM
        """
        return vm.get_cpu()

    def get_capacity(self):
        """Return subset CPU capacity.
        Capacity : number of physical CPU which can be used by VM

        Returns
        -------
        capacity : int
            Subset CPU capacity
        """
        return self.count_res()

    def get_current_resources_usage(self):
        """Get usage of physical CPU resources

        Returns
        -------
        Usage : float
            Percentage [0:1]
        """
        return self.cpu_explorer.get_usage_of(self.get_res())


    def get_current_consumer_usage(self, consumer : DomainEntity):
        """Get current CPU usage of a single consumer
        
        Parameters
        ----------
        consumer : DomainEntity
            The VM to consider

        Returns
        -------
        usage : float
            Percentage [0:1]
        """
        return self.connector.get_usage_cpu(consumer)

    def deploy(self, vm : DomainEntity):
        """Deploy a VM on CPU subset
        Should adapt the DomainEntity object as required before the subsetManager applies changes with connector
        ----------
        
        Parameters
        ----------
        vm : DomainEntity
            The VM to consider
        """
        success = super().deploy(vm) 
        # Update vm pinning
        self.sync_pinning()
        # Reset CPU time used to compute usage
        for server_cpu in self.res_list: server_cpu.get_hist().clear_time() # TODO: needed?
        return success

    def sync_pinning(self):
        """Synchronize VM pinning to CPU according to the current ServerCPU list
        ----------
        """
        template = self.connector.build_cpu_pinning(cpu_list=self.get_pinning_res(), host_config=self.cpu_count)
        for consumer in self.consumer_list:
            consumer.set_cpu_pin(template)
            if consumer.is_deployed() and not self.offline: self.connector.update_cpu_pinning(vm=consumer)

    def get_pinning_res(self):
        """Get the resources to use for synchronisation. May be reimplemented

        Returns
        -------
        list : ServerCPU list
            list of resources to use
        """
        return self.get_res()

    def __str__(self):
        return 'CpuSubset oc:' + str(self.oversubscription) + ' alloc:' + str(self.get_allocation()) + ' capacity:' + str(self.get_capacity()) +\
            ' res:' + str([str(cpu.get_cpu_id()) for cpu in self.get_res()]) +\
            ' vm:' + str([vm.get_name() for vm in self.get_consumers()])

class CpuElasticSubset(CpuSubset):

    """
    A CpuElasticSubset is an arbitrary group of physical CPU to which consumers (e.g. VMs) can be attributed
    Elastic refer to its capability to adapt its size based on usage:
    We distinguish list of resources from list of active resources
    ...

    Additional attributes
    ----------
    res_list : list
        List of physical resources
    hist_usage : list
        list of resource usage percentage
    hist_consumers_usage : list
        dict of consumer resource usage percentage


    Public Methods reimplemented/introduced
    -------
    todo()
        todo
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Additional attributes
        self.active_res = list()
        self.hist_usage = list()
        self.hist_consumers_usage = dict()
        # TODO: retrieve pre-existing records?
        # TODO: is hist still needed in this class? Predictor object attributes may be enough
        # Retrieve specific configuration
        self.MONITORING_WINDOW = int(os.getenv('SCL_ACT_MONITORING')) #records older than this value are progressively purged
        self.MONITORING_LEARNING = int(os.getenv('SCL_ACT_LEARNING')) 
        self.MONITORING_LEEWAY = int(os.getenv('SCL_ACT_LEEWAY'))
        self.predictor = PredictorCsoaa(monitoring_window=self.MONITORING_WINDOW, monitoring_learning=self.MONITORING_LEARNING, monitoring_leeway=self.MONITORING_LEEWAY)

    def get_pinning_res(self):
        """Get the resources to use for synchronisation. May be reimplemented

        Returns
        -------
        list : ServerCPU list
            list of resources to use
        """
        if self.active_res: return self.active_res
        return self.res_list

    def update_monitoring(self, timestamp : int):
        """Order a monitoring session on current subset with specified timestamp key
        Use endpoint_pool to load and store from the appropriate location
        ----------

        Parameters
        ----------
        timestamp : int
            The timestamp key
    
        Returns
        -------
        subset_usage : float
            resource usage percentage
        consumers_usage : dict
            consumer resource usage percentage
        clean_needed : bool
            If VM left out of the scope of the scheduler (without passing by manager), return True
        """
        subset_usage, consumers_usage, clean_needed = super().update_monitoring(timestamp=timestamp)
        self.manage_hist_records(timestamp=timestamp, subset_usage=subset_usage, consumers_usage=consumers_usage)
        if subset_usage is None:
            return subset_usage, consumers_usage, clean_needed

        # Update active resources
        next_peak = self.predictor.predict(timestamp=timestamp, current_resources=self.count_res(),\
            allocation=self.get_allocation(), metric=subset_usage)
        if next_peak != len(self.active_res):
            self.active_res = self.res_list[:next_peak]
            self.sync_pinning()

        return subset_usage, consumers_usage, clean_needed
        

    def manage_hist_records(self, timestamp, subset_usage, consumers_usage):
        """Add new records to the subset collection attributes and manage expired data
        ----------

        Parameters
        ----------
        timestamp : int
            The timestamp key
        subset_usage : float
            The last subset resource usage percentage
        consumers_usage : float
            The last consumers dict resource usage percentage
        """
        # Add global usage
        if subset_usage is not None: self.hist_usage.append((timestamp, subset_usage))
        self.__remove_from_list_expired_timestamp(timestamp=timestamp, list_of_timestamp_tuple=self.hist_usage)

        # Add consumers usage
        for consumer_uuid, usage_tuple in consumers_usage.items():
            __, consumer_usage = usage_tuple # tuple from endpoint is (DomainEntity, value)
            if consumer_usage is not None:
                if consumer_uuid not in self.hist_consumers_usage: self.hist_consumers_usage[consumer_uuid] = list()
                self.hist_consumers_usage[consumer_uuid].append((timestamp, consumer_usage))
            # Manage expired data
            if consumer_uuid in self.hist_consumers_usage: self.__remove_from_list_expired_timestamp(timestamp=timestamp, list_of_timestamp_tuple=self.hist_consumers_usage[consumer_uuid])

    def remove_consumer(self, consumer):
        """Remove a consumer from subset
        ----------

        Parameters
        ----------
        consumer : object
            The consumer to remove
        """
        super().remove_consumer(consumer=consumer)
        if (consumer is not None and consumer.get_uuid() in self.hist_consumers_usage): del self.hist_consumers_usage[consumer.get_uuid()]

    def __remove_from_list_expired_timestamp(self, timestamp, list_of_timestamp_tuple : list):
        """Parse a list of tuple where the first record is a timestamp and remove all values being older than
        timestamp - self.MONITORING_WINDOW
        ----------

        Parameters
        ----------
        timestamp : int
            The timestamp key
        list_of_timestamp_tuple : list
            List of tuple (timestamp, value)
        """
        records_to_remove = list()
        for record_tuple in list_of_timestamp_tuple:
            record_timestamp, __ = record_tuple
            if record_timestamp < (timestamp - self.MONITORING_WINDOW): records_to_remove.append(record_tuple)
            else: break # as values are parsed from older to newer ones
        for record_to_remove in records_to_remove: list_of_timestamp_tuple.remove(record_to_remove)

    def __str__(self):
        return 'CpuElasticSubset oc:' + str(self.oversubscription) + ' alloc:' + str(self.get_allocation()) + ' capacity:' + str(self.get_capacity()) +\
            ' res:' + str([str(cpu.get_cpu_id()) for cpu in self.get_res()]) +\
            ' active:' + str([str(cpu.get_cpu_id()) for cpu in self.active_res]) +\
            ' vm:' + str([vm.get_name() for vm in self.get_consumers()])

class MemSubset(Subset):
    """
    A MemSubset is an arbitrary division of memory to which consumers (e.g. VMs) can be attributed
    ...

    Public Methods reimplemented/introduced
    -------
    todo()
        todo
    """

    def __init__(self, **kwargs):
        additional_attributes = ['connector', 'mem_explorer']
        for req_attribute in additional_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', additional_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        super().__init__(**kwargs)

    def get_res_name(self):
        """Get resource name managed by susbset
        ----------

        Return
        ----------
        res : str
            resource name
        """
        return 'mem'

    def get_vm_allocation(self, vm : DomainEntity):
        """Return Memory allocation of a given VM without oversubscription consideration
        ----------

        Returns
        -------
        allocation : int
            Number of resources requested by the VM
        """
        return vm.get_mem(as_kb=False) # in MB

    def get_capacity(self):
        """Return subset memory capacity.
        Capacity : Amount of physical memory which can be used by VM

        Returns
        -------
        capacity : int
            Subset Memory capacity
        """
        capacity = 0
        for bound_inferior, bound_superior in self.res_list:
            capacity += (bound_superior-bound_inferior) 
        return capacity

    def get_current_resources_usage(self):
        """Get usage of physical Memory resources

        Returns
        -------
        Usage : int
            Percentage [0:1]
        """
        return self.mem_explorer.get_usage_of(self.get_res())

    def get_current_consumer_usage(self, consumer : DomainEntity):
        """Get current Memory usage of a single consumer

        Parameters
        ----------
        consumer : DomainEntity
            The VM to consider

        Returns
        -------
        usage : dict
            Percentage [0:1]
        """
        return self.connector.get_usage_mem(consumer) 

    def deploy(self, vm : DomainEntity):
        """Deploy a VM on memory subset
        Should adapt the DomainEntity object as required before the subsetManager applies changes with connector
        ----------
        
        Parameters
        ----------
        vm : DomainEntity
            The VM to consider
        """
        success = super().deploy(vm)
        # Nothing special to do on memory with libvirt
        return success

    def __str__(self):
        return 'MemSubset oc:' + str(self.oversubscription) + ' alloc:' + str(self.get_allocation()) + ' capacity:' + str(self.get_capacity()) +\
            ' res:' + str([str(mem_tuple[0]) + ':' + str(mem_tuple[1]) for mem_tuple in self.get_res()]) +\
            ' vm:' + str([vm.get_name() for vm in self.get_consumers()])
