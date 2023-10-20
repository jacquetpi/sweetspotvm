from schedulerlocal.subset.subset import SubsetCollection, Subset, CpuSubset, CpuElasticSubset, MemSubset
from schedulerlocal.domain.domainentity import DomainEntity
from schedulerlocal.node.cpuexplorer import CpuExplorer
from schedulerlocal.node.memoryexplorer import MemoryExplorer

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
        req_attributes = ['connector', 'endpoint_pool']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        self.collection = SubsetCollection()
    
    def deploy(self, vm : DomainEntity):
        """Deploy a VM to the appropriate subset
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
        if self.collection.contains_subset(self.get_appropriate_id(vm)):
            return self.__try_to_deploy_on_existing_subset(vm)
        return self.__try_to_deploy_on_new_subset(vm)

    def remove(self, vm : DomainEntity):
        """Remove a VM
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
        subset_id = self.get_appropriate_id(vm)
        if not self.collection.contains_subset(subset_id): return False
        subset = self.collection.get_subset(subset_id)
        subset.remove_consumer(vm)
        self.shrink_subset(subset)
        return True

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
        return self.collection.has_vm(vm)

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
        return self.collection.get_vm_by_name(name)

    def __try_to_deploy_on_existing_subset(self,  vm : DomainEntity):
        """Try to deploy a VM to an existing subset by extending it if required 
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
        targeted_subset = self.collection.get_subset(self.get_appropriate_id(vm))
        # Check if subset has available space
        additional_res_required = targeted_subset.get_additional_res_count_required_for_vm(vm)
        if additional_res_required <= 0:
            # No missing resources, can deploy the VM right away
            return targeted_subset.deploy(vm)
        else:
            # Missing resources on subset, try to allocate more
            extended = self.try_to_extend_subset(targeted_subset, additional_res_required)
            if not extended: return False 
            return targeted_subset.deploy(vm) 

    def __try_to_deploy_on_new_subset(self, vm : DomainEntity):
        """Try to deploy a VM to a new subset
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
        oversubscription = self.get_appropriate_id(vm)
        subset = self.try_to_create_subset(initial_capacity=self.get_request(vm), oversubscription=oversubscription)
        if subset == None: return False
        self.collection.add_subset(oversubscription, subset)
        return subset.deploy(vm)

    def try_to_extend_subset(self,  subset : Subset, amount : int):
        """Try to extend subset resource by the specified amount. Resource dependant. Must be reimplemented
        ----------

        Parameters
        ----------
        subset : SubSet
            The targeted subset
        amount : int
            Resources requested

        Returns
        -------
        success : bool
            Return success status of operation
        """
        raise NotImplementedError()

    def try_to_create_subset(self,  initial_capacity : int, oversubscription : float):
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

    def get_appropriate_id(self, vm : DomainEntity):
        """For a given VM, a subset ID typically corresponds to its premium policy. Must be reimplemented
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        subset_id : float
            premium policy
        """
        raise NotImplementedError()

    def get_request(self, vm : DomainEntity):
        """For a given VM, return its resource request. Resource dependant. Must be reimplemented
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        request : int
            VM resource request
        """
        raise NotImplementedError()


    def shrink(self):
        """Reduce subset capacity based on current allocation
        ----------
        """
        for subset in self.collection.get_subsets(): self.shrink_subset(subset)

    def shrink_subset(self, subset : Subset = None):
        """Reduce subset capacity based on current allocation. Resource dependant. Must be reimplemented
        ----------

        Parameters
        ----------
        subset : Subset
            The subset to shrink
        """
        raise NotImplementedError()

    def get_current_resources_usage(self):
        """Get current usage of physical resources. Resource dependant. Must be reimplemented

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
        clean_needed_list = self.collection.update_monitoring(timestamp=timestamp)
        for subset in clean_needed_list: self.shrink_subset(subset)

    def status(self):
        """Return susbset status as dict
        ----------

        Returns
        -------
        status : dicts
            Subset status
        """
        available = self.get_available_res_count()
        status = {'avail': available, 'subset': dict()}
        for name, subset in self.collection.get_dict().items():
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

    def get_capacity(self):
        """Get resource capacity managed by ManagerSubset. Resource dependant. Must be reimplemented
        ----------

        Return
        ----------
        capacity : float
            capacity as float
        """
        raise NotImplementedError()

    def get_available_res_count(self):
        """Get available resources count on ManagerSubset. Resource dependant. Must be reimplemented
        ----------

        Return
        ----------
        count : int
            resource count
        """
        raise NotImplementedError()

    def get_consumers(self):
        """Get List of hosted VMs
        ----------

        Return
        ----------
        vm : list
            List of hosted vm
        """
        return self.collection.get_consumers()


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
        super().__init__(**kwargs)

    def try_to_create_subset(self,  initial_capacity : int, oversubscription : float, subset_type : type = CpuSubset):
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
        available_cpus_ordered = self.__get_farthest_available_cpus()

        if len(available_cpus_ordered) < initial_capacity: return None
        starting_cpu = available_cpus_ordered[0]
        cpu_subset = subset_type(connector=self.connector, cpu_explorer=self.cpu_explorer, endpoint_pool=self.endpoint_pool,\
            oversubscription=oversubscription, cpu_count=self.cpuset.get_host_count(), offline=self.offline)
        cpu_subset.add_res(starting_cpu)

        initial_capacity-=1 # One was attributed
        if initial_capacity>0:
            available_cpus_ordered = self.__get_closest_available_cpus(cpu_subset) # Recompute based on chosen starting point
            for i in range(initial_capacity): cpu_subset.add_res(available_cpus_ordered[i])

        return cpu_subset

    def try_to_extend_subset(self, subset : CpuSubset, amount : int):
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
        available_cpus_ordered = self.__get_closest_available_cpus(subset)
        if len(available_cpus_ordered) < amount: return None
        subset.add_res(available_cpus_ordered[0])
        return self.try_to_extend_subset(subset,amount=(amount-1))

    def __get_closest_available_cpus(self, subset : CpuSubset):
        """Retrieve the list of available CPUs ordered by their average distance value closest to specified Subset
        ----------

        Parameters
        ----------
        subset : CpuSubset
            The subset requested

        Returns
        -------
        cpu_list : list
            List of available CPU ordered by their distance
        """
        cpuid_dict = {cpu.get_cpu_id():cpu for cpu in self.cpuset.get_cpu_list()}
        available_list = self.__get_available_cpus()
        allocated_list = subset.get_res()
        available_cpu_weighted = self.__get_available_cpus_with_weight(from_list=available_list, to_list=allocated_list, exclude_max=False)
        # Reorder distances from the closest one to the farthest one
        return [cpuid_dict[cpuid] for cpuid, v in sorted(available_cpu_weighted.items(), key=lambda item: item[1])]

    def __get_farthest_available_cpus(self):
        """When considering subset allocation. One may want to start from the farthest CPU possible
        This getter retrieve available CPUs and order them in a reverse order based on distance from current subsets CPUs
        ----------

        Returns
        -------
        ordered_cpu : list
            List of available CPU ordered in reverse by their distance
        """
        cpuid_dict = {cpu.get_cpu_id():cpu for cpu in self.cpuset.get_cpu_list()}
        available_list = self.__get_available_cpus()
        allocated_list = self.collection.get_res()
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

    def __get_available_cpus(self):
        """Retrieve the list of CPUs without subset attribution
        ----------

        Returns
        -------
        cpu_list : list
            list of CPUs without attribution
        """
        allocated_cpu_list = self.collection.get_res()
        available_cpu_list = list()
        for cpu in self.cpuset.get_cpu_list(): 
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

    def get_appropriate_id(self, vm : DomainEntity):
        """For a given VM, get its appropriate subset ID (corresponds to its premium policy)
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        id : int
            Its oversubscription ratio as subset ID
        """
        return vm.get_cpu_ratio()

    def get_current_resources_usage(self):
        """Get usage of physical CPU resources

        Returns
        -------
        Usage : int
            Percentage [0:1]
        """
        return self.cpu_explorer.get_usage_global()

    def get_request(self, vm : DomainEntity):
        """For a given VM, return its CPU request
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        cpu : int
            CPU request of given VM
        """
        return vm.get_cpu()

    def get_res_name(self):
        """Get resource name managed by ManagerSubset
        ----------

        Return
        ----------
        res : str
            resource name
        """
        return 'cpu'

    def get_capacity(self):
        """Get CPU capacity managed by ManagerSubset
        ----------

        Return
        ----------
        capacity : float
            capacity as float
        """
        return self.cpuset.get_allowed()

    def get_available_res_count(self):
        """Get available CPU count on CpuSubsetManager
        ----------

        Return
        ----------
        count : int
            available cpu count
        """
        return len(self.__get_available_cpus())

    def __str__(self):
        return 'CPUSubsetManager:\n' +  str(self.collection)

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

    def try_to_create_subset(self,  initial_capacity : int, oversubscription : float):
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
        return super().try_to_create_subset(initial_capacity=initial_capacity, oversubscription=oversubscription, subset_type=CpuElasticSubset)

    def __str__(self):
        return 'CPUElasticSubsetManager:\n' +  str(self.collection)

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
        super().__init__(**kwargs)

    def try_to_create_subset(self,  initial_capacity : int, oversubscription : float):
        """Try to create subset with specified capacity
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
            Return MemSubset created. None if failed.
        """
        targeted_inf = 0
        for subset_tuple in self.collection.get_res():
            bound_inf, bound_sup = subset_tuple
            if bound_sup > targeted_inf: targeted_inf = bound_sup+1
        new_tuple = (targeted_inf, targeted_inf+initial_capacity)
        
        if not self.__check_capacity_bound(bounds=new_tuple): return None
        if not self.__check_overlap(new_tuple=new_tuple): return None

        mem_subset = MemSubset(oversubscription=oversubscription, connector=self.connector, endpoint_pool=self.endpoint_pool, mem_explorer=self.mem_explorer)

        mem_subset.add_res(new_tuple)
        return mem_subset

    def try_to_extend_subset(self,  subset : MemSubset, amount : int):
        """Try to extend subset memory by the specified amount
        ----------

        Parameters
        ----------
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
        
        success = self.__check_capacity_bound(bounds=new_tuple) 
        if not success: return False

        success = self.__check_overlap(new_tuple=new_tuple, initial_tuple=initial_tuple) 
        if not success: return False
        
        if initial_tuple != None: subset.remove_res(initial_tuple)
        subset.add_res(new_tuple)
        return True

    def __check_capacity_bound(self, bounds : tuple):
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
        host_capacity = self.memset.get_allowed()
        if bounds[0] < 0 : return False
        if bounds[1] > host_capacity: return False
        return True

    def __check_overlap(self, new_tuple : tuple, initial_tuple : tuple = None):
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
        for other_tuple in self.collection.get_res():
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

    def get_appropriate_id(self, vm : DomainEntity):
        """For a given VM, get its appropriate subset ID (corresponds to its premium policy)
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        id : int
            Its oversubscription ratio as subset ID
        """
        return 1 # Memory is out of scope of this paper

    def get_current_resources_usage(self):
        """Get usage of physical Memory resources

        Returns
        -------
        Usage : int
            Percentage [0:1]
        """
        return self.mem_explorer.get_usage_global()

    def get_request(self, vm : DomainEntity):
        """For a given VM, return its memory request
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        mem : int
            Memory request of given VM
        """
        return vm.get_mem(as_kb=False) # in MB

    def get_res_name(self):
        """Get resource name managed by ManagerSubset
        ----------

        Return
        ----------
        res : str
            resource name
        """
        return 'mem'

    def get_capacity(self):
        """Get Memory capacity managed by ManagerSubset
        ----------

        Return
        ----------
        capacity : float
            capacity as float
        """
        return self.memset.get_allowed()

    def get_available_res_count(self):
        """Get available memory quantity on MemSubsetManager
        ----------

        Return
        ----------
        memory : int
            Memory as MB
        """
        allocation = 0
        for subset_tuple in self.collection.get_res():
            bound_inf, bound_sup = subset_tuple
            if bound_sup>bound_inf: allocation+= bound_sup - bound_inf
        return self.get_capacity() - allocation

    def __str__(self):
        return 'MemSubsetManager:\n' +  str(self.collection)

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
            'cpu': CpuSubsetManager(connector=self.connector, endpoint_pool=self.endpoint_pool, cpuset=self.cpuset, distance_max=50, offline=self.offline),\
            'mem': MemSubsetManager(connector=self.connector, endpoint_pool=self.endpoint_pool, memset=self.memset)
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
        treated = list()
        success = True
        reason = None
        for subset_manager in self.subset_managers.values():
            if not subset_manager.deploy(vm): 
                success = False
                reason = 'Not enough space on res ' + subset_manager.get_res_name()
                break
            treated.append(subset_manager)
        # If we succeed, the DOA DomainEntity was adapted according to the need of all subsetsManager. We apply changes using the connector
        if success and not vm.is_deployed() and not offline:
            success, reason = self.connector.create_vm(vm)
        if success: return (success, reason)
        # If one step failed, we have to remove VM from others subset
        for subset_manager in treated: 
            subset_manager.remove(vm)
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
            if not subset_manager.remove(vm): 
                success = False
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
        has_vm = 0
        for subset_manager in self.subset_managers.values():
            if subset_manager.has_vm(vm_copy): has_vm+=1
        if has_vm >0: return True
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
            vm = subset_manager.get_vm_by_name(name)
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
            status[name] =  manager.status()
        return status

    def list_vm(self):
        """Return list of hosted VM
        ----------

        Returns
        -------
        vm_list : list
            List of hosted vm
        """
        return self.subset_managers['cpu'].get_consumers()

    def __str__(self):
        return ''.join([str(subset_manager) + '\n' for subset_manager in self.subset_managers.values()])