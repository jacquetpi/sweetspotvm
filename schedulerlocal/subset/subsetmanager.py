from schedulerlocal.subset.subset import SubsetCollection, Subset, CpuSubset, CpuElasticSubset, MemSubset
from schedulerlocal.domain.domainentity import DomainEntity
from schedulerlocal.node.cpuexplorer import CpuExplorer
from schedulerlocal.node.memoryexplorer import MemoryExplorer
from schedulerlocal.subset.templateoversubscription import *
import math

class SubsetManager(object):
    """
    A SubsetManager is an object in charge of determining appropriate subset collection
    ...

    Attributes
    ----------
    subset_collection : SubsetCollection
        collection of subset

    Public Methods
    -------
    build_initial_subset()
        Deploy a VM to the appropriate subset. Must be reimplemented
    """

    def __init__(self, **kwargs):
        req_attributes = ['connector', 'numa_id_list', 'endpoint_pool']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        self.collections = {numa_id: SubsetCollection() for numa_id in self.numa_id_list}

    def deploy(self, numa_id : int, vm : DomainEntity):
        """Deploy a VM to the appropriate subset
        ----------

        ParametersCpu
        ----------
        numa_id : int
            The numa node (identified by its id) to consider
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        success : bool
            Return success status of operation
        """
        for res_id, (subset_id, res_quantity) in enumerate(self.template_manager.get_subsets_for(vm)):
            success = self.__deploy_internal(vm=vm,numa_id=numa_id,res_id=res_id,subset_id=subset_id,res_quantity=res_quantity)
            if not success: break
        return success

    def __deploy_internal(self, numa_id : int, vm : DomainEntity, res_id : int, subset_id : float, res_quantity : float):
        if self.collections[numa_id].contains_subset(subset_id):
            return self.__try_to_deploy_on_existing_subset(numa_id=numa_id,vm=vm,res_id=res_id,subset_id=subset_id,res_quantity=res_quantity)
        return self.__try_to_deploy_on_new_subset(numa_id=numa_id,vm=vm,res_id=res_id,subset_id=subset_id,res_quantity=res_quantity)

    def remove(self, numa_id : int, vm : DomainEntity):
        """Remove a VM
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        success : bool
            Return success status of operation
        """
        for subset in self.collections[numa_id].get_subsets():
            if subset.has_vm(vm, ignore_destroyed=False): # As we look for a VM being in the destroy process
                subset.remove_consumer(vm)
                self.shrink_subset(subset)
        return True

    def has_vm(self, numa_id : int, vm : DomainEntity):
        """Test if a VM is present in a subset
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider        
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        success : bool
            Return success status of operation
        """
        return self.collections[numa_id].has_vm(vm)

    def get_vm_by_name(self, numa_id : int, name : str):
        """Get a vm by its name, none if not present
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider        
        name : str
            The name to search for

        Returns
        -------
        vm : DomainEntity
            None if not present
        """
        return self.collections[numa_id].get_vm_by_name(name)

    def __try_to_deploy_on_existing_subset(self, numa_id : int, vm : DomainEntity, res_id : int, subset_id : float, res_quantity : float):
        """Try to deploy a VM to an existing subset by extending it if required 
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider        
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        success : bool
            Return success status of operation
        """
        targeted_subset = self.collections[numa_id].get_subset(subset_id)
        # Check if subset has available space
        additional_res_required = targeted_subset.get_additional_res_count_required_for_quantity(vm=vm,quantity=res_quantity)
        if additional_res_required <= 0:
            # No missing resources, can deploy the VM right away
            return targeted_subset.deploy(vm, res_id, res_quantity)
        else:
            # Missing resources on subset, try to allocate more
            extended = self.try_to_extend_subset(numa_id=numa_id,subset=targeted_subset, amount=additional_res_required)
            if not extended: return False
            return targeted_subset.deploy(vm, res_id, res_quantity) 

    def __try_to_deploy_on_new_subset(self, numa_id : int, vm : DomainEntity, res_id : int, subset_id : float, res_quantity : float):
        """Try to deploy a VM to a new subset
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider        
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        success : bool
            Return success status of operation
        """
        subset = self.try_to_create_subset(numa_id=numa_id,initial_capacity=res_quantity, oversubscription=subset_id)
        if subset == None: return False
        self.collections[numa_id].add_subset(subset_id, subset)
        return subset.deploy(vm, res_id, res_quantity) 

    def try_to_extend_subset(self, numa_id : int, subset : Subset, amount : int):
        """Try to extend subset resource by the specified amount. Resource dependant. Must be reimplemented
        ----------

        Parameters
        ----------
        subset : Subset
            The targeted subset
        amount : int
            Resources requested

        Returns
        -------
        success : bool
            Return success status of operation
        """
        raise NotImplementedError()

    def try_to_create_subset(self, numa_id : int, initial_capacity : int, oversubscription : float):
        """Try to create subset with specified capacity. Resource dependant. Must be reimplemented
        ----------

        Parameters
        ----------
        initial_capacity : int
            Resources requested
        oversubscription : float
            Subset oversubscription

        Returns
        -------
        subset : Subset
            Return Subset created. None if failed. Resource dependant. Must be reimplemented
        """
        raise NotImplementedError()

    def shrink(self, numa_id : int):
        """Reduce subset capacity based on current allocation
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider
        """
        for subset in self.collections[numa_id].get_subsets(): self.shrink_subset(subset)

    def shrink_subset(self, subset : Subset = None):
        """Reduce subset capacity based on current allocation. Resource dependant. Must be reimplemented
        ----------

        Parameters
        ----------
        subset : Subset
            The subset to shrink
        """
        raise NotImplementedError()

    def get_current_resources_usage(self, numa_id : int):
        """Get current usage of physical resources. Resource dependant. Must be reimplemented

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider

        Returns
        -------
        usage : int
            Percentage [0:1]
        """
        raise NotImplementedError()

    def iterate(self, timestamp : int):
        """Order a monitoring session on host resources and on each subset with specified timestamp key
        Use endpoint_pool to load and store from the appropriate location
        ----------

        Parameters
        ----------
        timestamp : int
            The timestamp key
        """
        # Update global data: Nothing is done live with it but data are dumped for post analysis
        data = self.endpoint_pool.load_global(timestamp=timestamp, subset_manager=self)
        # Update subset data
        clean_needed_list = list()
        for collection in self.collections.values():
            clean_needed_list.extend(collection.update_monitoring(timestamp=timestamp))
        for subset in clean_needed_list: self.shrink_subset(subset)

    def status(self, numa_id : int):
        """Return susbset status as dict
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider

        Returns
        -------
        status : dicts
            Subset status
        """
        available = self.get_available_res_count(numa_id=numa_id)
        status = {'avail': available, 'subset': dict()}
        for name, subset in self.collections[numa_id].get_dict().items():
            status['subset'][name] = subset.status()
            status['subset'][name]['vpotential'] = subset.get_oversubscription().get_oversubscribed_quantity(quantity=available, with_new_vm=True)
        return status

    def get_res_name(self):
        """Get resource name managed by ManagerSubset. Resource dependant. Must be reimplemented
        ----------

        Return
        ----------
        res : str
            resource name
        """
        raise NotImplementedError()

    def get_capacity(self, numa_id : int):
        """Get resource capacity managed by ManagerSubset on numa node. Resource dependant. Must be reimplemented
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider

        Return
        ----------
        capacity : float
            capacity as float
        """
        raise NotImplementedError()

    def get_available_res_count(self, numa_id : int):
        """Get available resources count on ManagerSubset. Resource dependant. Must be reimplemented
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider

        Return
        ----------
        count : int
            resource count
        """
        raise NotImplementedError()

    def get_consumers(self, numa_id : int):
        """Get List of hosted VMs
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider

        Return
        ----------
        vm : list
            List of hosted vm
        """
        return self.collections[numa_id].get_consumers()

    def get_numa_ids(self):
        """Get List of numa node ids related to resource 
        ----------

        Return
        ----------
        numa_id_list : list
            List of numa ids
        """
        return self.numa_id_list

class CpuSubsetManager(SubsetManager):
    """
    A CpuSubsetManager is an object in charge of determining appropriate CPU subset collection
    ...

    Attributes
    ----------
    subset_collection : SubsetCollection
        collection of subset

    Public Methods reimplemented/introduced
    -------
    deploy()
        Deploy a VM to the appropriate CPU subset
    """
    def __init__(self, **kwargs):
        req_attributes = ['connector', 'cpuset', 'distance_max', 'offline']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        self.cpu_explorer = CpuExplorer()
        self.template_manager = TemplateOversubscriptionCpu()
        super().__init__(**kwargs)

    def deploy(self, numa_id : int, vm : DomainEntity):
        """Deploy a VM to the appropriate subset
        ----------

        ParametersCpu
        ----------
        numa_id : int
            The numa node (identified by its id) to consider
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        success : bool
            Return success status of operation
        """
        success = super().deploy(numa_id=numa_id,vm=vm)
        if success: self.balance_available_resources()
        return success

    def remove(self, numa_id : int, vm : DomainEntity):
        """Remove a VM from the appropriate subset
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        success : bool
            Return success status of operation
        """
        success = super().remove(numa_id=numa_id,vm=vm)
        if success: self.balance_available_resources()
        return success

    def try_to_create_subset(self,  numa_id : int, initial_capacity : int, oversubscription : float, subset_type : type = CpuSubset):
        """Try to create subset with specified capacity
        ----------

        Parameters
        ----------
        initial_capacity : int
            Resources requested (without oversubscription consideration)
        oversubscription : float
            Subset oversubscription

        Returns
        -------
        subset : Subset
            Return CpuSubset created. None if failed.
        """
        if initial_capacity <=0 : raise ValueError('Cannot create a subset with negative capacity', initial_capacity)
        
        # Starting point
        available_cpus_ordered = self.__get_farthest_available_cpus(numa_id=numa_id)

        if len(available_cpus_ordered) < initial_capacity: return None
        starting_cpu = available_cpus_ordered[0]
        cpu_subset = subset_type(connector=self.connector, cpu_explorer=self.cpu_explorer, endpoint_pool=self.endpoint_pool,\
            oversubscription=oversubscription, cpu_count=self.cpuset.get_host_count(), numa_id=numa_id, offline=self.offline)
        cpu_subset.add_res(starting_cpu)

        initial_capacity-=1 # One was attributed
        if initial_capacity>0:
            available_cpus_ordered = self.__get_closest_available_cpus(numa_id=numa_id, subset=cpu_subset) # Recompute based on chosen starting point
            for i in range(initial_capacity): cpu_subset.add_res(available_cpus_ordered[i])

        return cpu_subset

    def try_to_extend_subset(self, numa_id : int, subset : CpuSubset, amount : int):
        """Try to extend subset cpu by the specified amount
        ----------

        Parameters
        ----------
        subset : SubSet
            The amount requested
        amount : int
            Resources requested

        Returns
        -------
        success : bool
            Return success status of operation
        """
        if amount<=0: return True
        available_cpus_ordered = self.__get_closest_available_cpus(numa_id=numa_id,subset=subset)
        if len(available_cpus_ordered) < amount: return False # should not be possible
        subset.add_res(available_cpus_ordered[0])
        return self.try_to_extend_subset(numa_id=numa_id,subset=subset,amount=(amount-1))

    def balance_available_resources(self):
        """If critical size is not reached on an oversubscribed subset and available resources are present, distribute them
        ----------
        """
        # Retrieve data from context
        capacity_oversub   = 0
        allocation_oversub = 0
        allocation_oversub_list  = list()
        oversub_list  = list()
        critical_size_unreached  = False
        min_oversubscribed_level = None
        for numa_id in self.numa_id_list:
            for level, subset in self.collections[numa_id].get_dict().items():
                if level <= 1.0:
                    continue
                else:
                    capacity_oversub   += subset.get_capacity()
                    allocation_oversub += subset.get_allocation()
                    allocation_oversub_list.extend(subset.get_res())
                    oversub_list.append(subset)
                    critical_size_unreached = critical_size_unreached or (not subset.get_oversubscription().is_critical_size_reached())
                    if (min_oversubscribed_level == None) or (level < min_oversubscribed_level): min_oversubscribed_level = level

        # Test if balance is useful/possible
        if critical_size_unreached:
            min_allocation_for_mutualisation =  math.ceil(allocation_oversub/min_oversubscribed_level)

            potential_allocation = allocation_oversub
            for numa_id in self.numa_id_list: potential_allocation+=self.get_available_res_count(numa_id=numa_id)

            if potential_allocation >= min_allocation_for_mutualisation:

                for numa_id in self.numa_id_list: allocation_oversub_list.extend(self.__get_available_cpus(numa_id=numa_id))
                for subset in oversub_list: subset.sync_pinning(cpu_list=allocation_oversub_list)


    def __get_closest_available_cpus(self, numa_id : int, subset : CpuSubset):
        """Retrieve the list of available CPUs ordered by their average distance value closest to specified Subset
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider
        subset : CpuSubset
            The subset requested

        Returns
        -------
        cpu_list : list
            List of available CPU ordered by their distance
        """
        cpuid_dict = {cpu.get_cpu_id():cpu for cpu in self.cpuset.get_numa_cpu_list(numa_id=numa_id)}
        available_list = self.__get_available_cpus(numa_id=numa_id)
        available_cpu_weighted = self.__get_available_cpus_with_weight(from_list=available_list, to_list=subset.get_res(), exclude_max=False)

        # Now, we penalize cores that are closer to others subset
        penalty = max(available_cpu_weighted.values()) if available_cpu_weighted else 0
        for other_subset in self.collections[numa_id].get_subsets():
            if other_subset.get_oversubscription_id() == subset.get_oversubscription_id(): continue

            other_cpu_weighted = self.__get_available_cpus_with_weight(from_list=available_list, to_list=other_subset.get_res(), exclude_max=False)
            for cpuid in available_cpu_weighted.keys():
                if other_cpu_weighted[cpuid] < available_cpu_weighted[cpuid]: available_cpu_weighted[cpuid] += penalty
        
        # Reorder distances from the closest one to the farthest one
        return [cpuid_dict[cpuid] for cpuid, v in sorted(available_cpu_weighted.items(), key=lambda item: item[1])]

    def __get_farthest_available_cpus(self, numa_id : int):
        """When considering subset allocation. One may want to start from the farthest CPU possible
        This getter retrieve available CPUs and order them in a reverse order based on distance from current subsets CPUs
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider

        Returns
        -------
        ordered_cpu : list
            List of available CPU ordered in reverse by their distance
        """
        cpuid_dict = {cpu.get_cpu_id():cpu for cpu in self.cpuset.get_cpu_list()}
        available_list = self.__get_available_cpus(numa_id=numa_id)
        allocated_list = self.collections[numa_id].get_res()
        available_cpu_weighted = self.__get_available_cpus_with_weight(from_list=available_list, to_list=allocated_list, exclude_max=False)
        # Reorder distances from the farthest one to the closest one
        return [cpuid_dict[cpuid] for cpuid, v in sorted(available_cpu_weighted.items(), key=lambda item: item[1], reverse=True)]

    def __get_available_cpus_with_weight(self, from_list : list, to_list : list, exclude_max : bool = True):
        """Computer the average distance of CPU presents in from_list to the one in to_list
        ----------

        Parameters
        ----------
        from_list : list
            list of ServerCPU
        to_list : list
            list of ServerCPU
        exclude_max : bool (optional)
            Should CPU having a distance value higher than the one fixed in max_distance attribute being disregarded
        
        Returns
        -------
        distance : dict
            Dict of CPUID (as key) with average distance being computed
        """
        computed_distances = dict()
        for available_cpu in from_list:
            total_distance = 0
            total_count = 0

            exclude_identical = False
            for subset_cpu in to_list:
                if subset_cpu == available_cpu: 
                    exclude_identical = True
                    break

                distance = self.cpuset.get_distance_between_cpus(subset_cpu, available_cpu)
                if exclude_max and (distance >= self.distance_max): continue

                total_distance+=distance
                total_count+=1

            if exclude_identical : continue
            if total_count <= 0: computed_distances[available_cpu.get_cpu_id()] = 0
            elif total_distance>=0: computed_distances[available_cpu.get_cpu_id()] = total_distance/total_count

        return computed_distances

    def __get_available_cpus(self, numa_id : int):
        """Retrieve the list of CPUs without subset attribution
        ----------

        Returns
        -------
        cpu_list : list
            list of CPUs without attribution
        """
        allocated_cpu_list = self.collections[numa_id].get_res()
        available_cpu_list = list()
        for cpu in self.cpuset.get_numa_cpu_list(numa_id=numa_id): 
            if cpu not in allocated_cpu_list: available_cpu_list.append(cpu)
        return available_cpu_list

    def shrink_subset(self, subset : CpuSubset):
        """Reduce subset capacity based on current allocation
        ----------

        Parameters
        ----------
        subset : Subset
            The subset to shrink
        """
        unused = subset.unused_resources_count()
        res_list = list(subset.get_res())
        last_index = len(res_list) - 1
        for count in range(unused): subset.remove_res(res_list[last_index-count])
        subset.sync_pinning()

    def get_current_global_resources_usage(self):
        """Get usage of physical CPU resources

        Returns
        -------
        Usage : int
            Percentage [0:1]
        """
        return self.cpu_explorer.get_usage_global()

    def get_res_name(self):
        """Get resource name managed by ManagerSubset
        ----------

        Return
        ----------
        res : str
            resource name
        """
        return 'cpu'

    def get_capacity(self, numa_id : int = None):
        """Get CPU capacity managed by ManagerSubset on numa node
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider

        Return
        ----------
        capacity : float
            capacity as float
        """
        if numa_id == None:
            return self.cpuset.get_allowed()
        return self.cpuset.get_numa_allowed(numa_id=numa_id)

    def get_available_res_count(self, numa_id : int):
        """Get available CPU count on CpuSubsetManager
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider

        Return
        ----------
        count : int
            available cpu count
        """
        return len(self.__get_available_cpus(numa_id=numa_id))

    def __str__(self):
        text = ""
        for numa_id in self.numa_id_list: text+= 'CPUSubsetManager ' + str(numa_id) + ':\n' +  str(self.collections[numa_id])
        return text

class CpuElasticSubsetManager(CpuSubsetManager):
    """
    An CpuElasticSubsetManager is a CpuSubsetManager which manage an elastic subset
    Traditional subsets size are only changed at new deployment/deletion.
    An elastic subset size is continously adapted
    ...

    Public Methods reimplemented/introduced
    -------
    iterate()
        Monitoring session and size adjustement of subset
    """

    def try_to_create_subset(self, numa_id : int, initial_capacity : int, oversubscription : float):
        """Try to create subset with specified capacity
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider
        initial_capacity : int
            Resources requested (without oversubscription consideration)
        oversubscription : float
            Subset oversubscription

        Returns
        -------
        subset : Subset
            Return CpuSubset created. None if failed.
        """
        return super().try_to_create_subset(numa_id=numa_id,initial_capacity=initial_capacity, oversubscription=oversubscription, subset_type=CpuElasticSubset)

    def __str__(self):
        text = ""
        for numa_id in self.numa_id_list: text+= 'CPUElasticSubsetManager ' + str(numa_id) + ':\n' +  str(self.collections[numa_id])
        return text

class MemSubsetManager(SubsetManager):
    """
    A MemSubsetManager is an object in charge of determining appropriate Memory subset collection
    /!\ : out of scope of this paper. We just expose memory as a single package
    ...

    Attributes
    ----------
    subset_collection : SubsetCollection
        collection of subset

    Public Methods reimplemented/introduced
    -------
    deploy()
        Deploy a VM to the appropriate CPU subset
    """
    def __init__(self, **kwargs):
        req_attributes = ['connector', 'memset']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        self.mem_explorer = MemoryExplorer()
        self.template_manager = TemplateOversubscriptionMem()
        super().__init__(**kwargs)

    def try_to_create_subset(self, numa_id : int, initial_capacity : int, oversubscription : float):
        """Try to create subset with specified capacity
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider
        initial_capacity : int
            Resources requested
        oversubscription : float
            Subset oversubscription

        Returns
        -------
        subset : Subset
            Return MemSubset created. None if failed.
        """
        targeted_inf = 0
        for subset_tuple in self.collections[numa_id].get_res():
            bound_inf, bound_sup = subset_tuple
            if bound_sup > targeted_inf: targeted_inf = bound_sup+1
        new_tuple = (targeted_inf, targeted_inf+initial_capacity)
        
        if not self.__check_capacity_bound(numa_id=numa_id,bounds=new_tuple): return None
        if not self.__check_overlap(numa_id=numa_id, new_tuple=new_tuple): return None

        mem_subset = MemSubset(oversubscription=oversubscription, numa_id=numa_id, connector=self.connector, endpoint_pool=self.endpoint_pool, mem_explorer=self.mem_explorer)

        mem_subset.add_res(new_tuple)
        return mem_subset

    def try_to_extend_subset(self, numa_id : int, subset : MemSubset, amount : int):
        """Try to extend subset memory by the specified amount
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider
        subset : SubSet
            The targeted subset
        amount : int
            Resources requested

        Returns
        -------
        success : bool
            Return success status of operation
        """
        if subset.get_res():
            initial_tuple = subset.get_res()[0]
            bound_inf, bound_sup = initial_tuple
            new_tuple = (bound_inf, bound_sup+amount)
        else:
            initial_tuple = None
            new_tuple = (0, amount)
        
        success = self.__check_capacity_bound(numa_id=numa_id, bounds=new_tuple) 
        if not success: return False

        success = self.__check_overlap(numa_id=numa_id, new_tuple=new_tuple, initial_tuple=initial_tuple) 
        if not success: return False
        
        if initial_tuple != None: subset.remove_res(initial_tuple)
        subset.add_res(new_tuple)
        return True

    def __check_capacity_bound(self, numa_id : int, bounds : tuple):
        """Check if specified extension (as tuple of bounds) verify host capacity
        ----------

        Parameters
        ----------
        bounds : tuple
            Bounds of memory

        Returns
        -------
        res : boolean
            True if host capacity handles extension. False otherwise.
        """
        host_capacity = self.memset.get_numa_allowed(numa_id)
        if bounds[0] < 0 : return False
        if bounds[1] > host_capacity: return False
        return True

    def __check_overlap(self, numa_id : int, new_tuple : tuple, initial_tuple : tuple = None):
        """Check if specified tuple modification overlaps with others memsubset
        ----------

        Parameters
        ----------
        initial_tuple : int
            initial tuple
        new_tuple : int
            Tuple modified

        Returns
        -------
        res : boolean
            False if overlap check failed, True if succeeded.
        """
        for other_tuple in self.collections[numa_id].get_res():
            if other_tuple == initial_tuple: continue
            overlap = max(0, min(new_tuple[1], other_tuple[1]) - max(new_tuple[0], other_tuple[0]))
            if overlap>0: return False
        return True

    def shrink_subset(self, subset : MemSubset = None):
        """Reduce subset capacity based on current allocation
        ----------

        Parameters
        ----------
        subset : Subset (opt)
            The subset to shrink(if not specified, all subset will be shrinked)
        """
        if not subset.get_res(): return # nothing to reduce
        unused = subset.unused_resources_count()
        initial_tuple = subset.get_res()[0]
        subset.remove_res(initial_tuple)
        if unused < initial_tuple[1]: subset.add_res((initial_tuple[0], initial_tuple[1]-unused))

    def get_current_global_resources_usage(self):
        """Get usage of physical Memory resources

        Returns
        -------
        Usage : int
            Percentage [0:1]
        """
        return self.mem_explorer.get_usage_global()

    def get_res_name(self):
        """Get resource name managed by ManagerSubset
        ----------

        Return
        ----------
        res : str
            resource name
        """
        return 'mem'

    def get_capacity(self, numa_id : int = None):
        """Get Memory capacity managed by ManagerSubset on numa node
        ----------

        Parameters
        ----------
        numa_id : int
            The numa node (identified by its id) to consider

        Return
        ----------
        capacity : float
            capacity as float
        """
        if numa_id == None:
            return self.memset.get_allowed()
        return self.memset.get_numa_allowed(numa_id)

    def get_available_res_count(self, numa_id : int):
        """Get available memory quantity on MemSubsetManager
        ----------

        Return
        ----------
        memory : int
            Memory as MB
        """
        allocation = 0
        for subset_tuple in self.collections[numa_id].get_res():
            bound_inf, bound_sup = subset_tuple
            if bound_sup>bound_inf: allocation+= bound_sup - bound_inf
        return self.get_capacity(numa_id=numa_id) - allocation

    def __str__(self):
        text = ""
        for numa_id in self.numa_id_list: text+= 'MemSubsetManager ' + str(numa_id) + ':\n' +  str(self.collections[numa_id])
        return text

class SubsetManagerPool(object):
    """
    A SubsetManagerPool is a pool of SubsetManager
    It is for now composed of a CpuSubsetManager and a MemSubsetManager
    /!\ Mem is out of scope of this paper. We just expose memory as a single package
    ...

    Attributes
    ----------
    cpu_subset_manager : CpuSubsetManager
        Cpu subset manager
    mem_subset_manager : MemSubsetManager
        Mem subset manager

    Public Methods
    -------
    iteration()
        Manage iteration
    """

    def __init__(self, **kwargs):
        req_attributes = ['connector', 'endpoint_pool', 'cpuset', 'memset', 'offline']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        self.subset_managers = {
            'cpu': CpuSubsetManager(connector=self.connector, endpoint_pool=self.endpoint_pool, numa_id_list=self.cpuset.get_numa_keys(), cpuset=self.cpuset, distance_max=50, offline=self.offline),\
            'mem': MemSubsetManager(connector=self.connector, endpoint_pool=self.endpoint_pool, numa_id_list=self.memset.get_numa_keys(), memset=self.memset)
            }
        self.watch_out_of_schedulers_vm() # Manage pre-installed VMs

    def iterate(self, timestamp : int, offline : bool = False):
        """Iteration : update monitoring of subsets and adjust size of elastic ones
        Print to the console current status if context has changed

        Parameters
        ----------
        timestamp : int
            Timestamp to use for monitoring session
        ----------
        """
        for subset_manager in self.subset_managers.values():
            subset_manager.iterate(timestamp=timestamp)
        # Print status to console if context changed
        status_str = str(self)
        if not hasattr(self, 'prev_status_str') or getattr(self, 'prev_status_str') != status_str: print(status_str)
        setattr(self, 'prev_status_str', status_str)

    def deploy(self, vm : DomainEntity, offline : bool = False):
        """Deploy a VM on subset managers
        ----------
        
        Parameters
        ----------
        vm : DomainEntity
            The VM to deploy

        Returns
        -------
        tuple : (bool, reason)
            Success as True/False with reason
        """
        for numa_id in self.subset_managers['cpu'].get_numa_ids():
            success = True
            treated = list()
            for subset_manager in self.subset_managers.values():
                if subset_manager.deploy(numa_id=numa_id,vm=vm):
                    treated.append(subset_manager)
                else:
                    success = False
                    reason = 'Not enough space on res ' + subset_manager.get_res_name()
                    # If one step failed, we have to remove VM from others subset
                    for subset_manager in treated: subset_manager.remove(numa_id=numa_id, vm=vm)
                    break # we must retry on other numa node
            if success: 
                reason = None
                break

        # If we succeed, the DOA DomainEntity was adapted according to the need of all subsetsManager. We apply changes using the connector
        if success and not vm.is_deployed() and not offline:
            success, reason = self.connector.create_vm(vm)
        return (success, reason)

    def remove(self, vm : DomainEntity = None, name : str = None, offline : bool = False):
        """Remove a VM from subset managers
        ----------
        
        Parameters
        ----------
        vm : DomainEntity
            The VM to remove identified as DomainEntity
        name : str
            The VM to remove identified by its name

        Returns
        -------
        tuple : (bool, reason)
            Success as True/False with reason
        """
        if name != None: vm = self.get_vm_by_name(name)
        if vm == None: return (False, 'does not exist')
        vm.set_being_destroyed(True)
        treated = list()
        success = True
        # First, remove from subsets
        for subset_manager in self.subset_managers.values():
            success = False
            for numa_id in subset_manager.get_numa_ids():
                if (subset_manager.has_vm(numa_id=numa_id,vm=vm)):
                    success = subset_manager.remove(numa_id=numa_id,vm=vm)
                    print('removing', vm.get_name(), success)
                    break

            if not success: 
                break
            treated.append(subset_manager)
        if not success:
            vm.set_being_destroyed(False)
            return (False, 'unable to remove it from all subsets')
        # second, remove from connector
        if not offline: (success, reason) = self.connector.delete_vm(vm)
        else: (success, reason) = (True, 'offline')
        if success:
            del vm
            return (success, reason)
        else: return (success, reason)

    def watch_out_of_schedulers_vm(self):
        """Treat VM deployed outside the scheduler
        ----------
        """
        for vm in self.connector.get_vm_alive_as_entity():
            if vm.is_being_destroyed(): continue
            if not self.has_vm(vm):
                success_tuple = self.deploy(vm)
                print('Warning: VM deployed out of scope of this scheduler detected ', vm.get_name(), ' was integrated:', success_tuple)

    def has_vm(self, vm_copy : DomainEntity):
        """Test if a VM is present in subsetManagers
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        success : bool
            Return if vm was found
        """
        for subset_manager in self.subset_managers.values():
            for numa_id in subset_manager.get_numa_ids():
                if(subset_manager.has_vm(numa_id=numa_id,vm=vm_copy)):
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
        has_vm = 0
        being_destroyed = False
        found = None
        for subset_manager in self.subset_managers.values():

            vm = None
            for numa_id in subset_manager.get_numa_ids():
                vm = subset_manager.get_vm_by_name(numa_id=numa_id,name=name)
                if vm != None: break

            if vm != None: 
                has_vm+=1
                being_destroyed = being_destroyed or vm.is_being_destroyed()
                found = vm

        if (has_vm != len(self.subset_managers)) and (has_vm != 0):
            based_message = 'Warning: vm ' + name + ' unequally present in subsets'
            if not being_destroyed: print(based_message)
            else: print(based_message  + ' while being destroyed')
        return found

    def status(self):
        """Return susbsets status as dict
        ----------

        Returns
        -------
        status : dicts
            Subset status
        """
        status = dict()
        for name, manager in self.subset_managers.items():
            status[name] = dict()
            for numa_id in manager.get_numa_ids():
                status[name][numa_id] =  manager.status(numa_id=numa_id)
        return status

    def list_vm(self):
        """Return list of hosted VM
        ----------

        Returns
        -------
        vm_list : list
            List of hosted vm
        """
        consumers = list()
        for numa_id in self.subset_managers['cpu'].get_numa_ids():
            consumers.extend[self.subset_managers['cpu'].get_consumers(numa_id=numa_id)]
        return consumers

    def __str__(self):
        return ''.join([str(subset_manager) + '\n' for subset_manager in self.subset_managers.values()])