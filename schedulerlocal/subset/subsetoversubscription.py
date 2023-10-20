from schedulerlocal.domain.domainentity import DomainEntity
from math import ceil, floor
import os

class SubsetOversubscription(object):
    """
    A SubsetOversubscription class is in charge to apply a specific oversubscription mechanism to a given subset
    ...

    Public Methods
    -------
    get_available()
        Virtual resources available
    unused_resources_count()
        Return attributed physical resources which are unused
    get_additional_res_count_required_for_vm()
        Return count of additional physical resources needed to deploy a vm
    get_id()
        Return oversubscription id
    """
    def __init__(self, **kwargs):
        req_attributes = ['subset']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])

    def get_available(self):
        """Return the number of virtual resources unused
        ----------

        Returns
        -------
        available : int
            count of available resources
        """
        raise NotImplementedError()

    def unused_resources_count(self):
        """Return attributed physical resources which are unused
        ----------

        Returns
        -------
        unused : int
            count of unused resources
        """
        raise NotImplementedError()

    def get_id(self):
        """Return the oversubscription strategy ID
        ----------

        Returns
        -------
        id : str
           oversubscription id
        """
        raise NotImplementedError()

class SubsetOversubscriptionStatic(SubsetOversubscription):
    """
    A SubsetOversubscriptionStatic implements a static oversubscription mechanism (i.e. resource are oversubscribed by a fixed ratio)

    Attributes
    ----------
    ratio : float
        Static oversubscription ratio to apply
    critical_size : int
        VM will start being oversubscribed only after the number of VM reaches the critical_size attribute
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        req_attributes = ['ratio']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        self.critical_size = int(os.getenv('OVSB_CRITICAL_SIZE'))

    def get_available(self, with_new_vm : bool = False):
        """Return the number of virtual resource available
        ----------

        Parameters
        ----------
        with_new_vm : bool (opt)
            If a new VM is to be considered while computing if critical size is reached

        Returns
        -------
        available : int
            count of available resources
        """
        return self.get_oversubscribed_quantity(quantity=self.subset.get_capacity(), with_new_vm=with_new_vm) - self.subset.get_allocation()

    def get_oversubscribed_quantity(self, quantity : int, with_new_vm : bool = False):
        """Based on a specific quantity, return oversubscribed equivalent
        ----------

        Parameters
        ----------
        quantity : int
            Quantity to be oversubscribed
        with_new_vm : bool (opt)
            If a new VM is to be considered while computing if critical size is reached

        Returns
        -------
        quantity : int
            Quantity oversubscribed
        """
        return quantity*self.__get_effective_ratio(with_new_vm)

    def unused_resources_count(self):
        """Return attributed physical resources which are unused
        ----------

        Returns
        -------
        unused : int
            count of unused resources
        """
        available_oversubscribed = self.get_available()
        unused_cpu = floor(available_oversubscribed/self.__get_effective_ratio())

        used_cpu = self.subset.get_capacity() - unused_cpu

        # Test specific case: our unused count floor should not reduce the capacity below the maximum configuration observed
        # Avoid VM to be oversubscribed with themselves
        max_alloc = self.subset.get_max_consumer_allocation()
        if used_cpu < max_alloc: return max(0, floor(self.subset.get_capacity()-max_alloc))
    
        # Generic case
        return unused_cpu


    def get_additional_res_count_required_for_vm(self, vm : DomainEntity):
        """Return the number of additional physical resource required to deploy specified vm. 
        0 if no additional resources is required
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
        request    = self.subset.get_vm_allocation(vm) # Without oversubscription
        capacity   = self.subset.get_capacity() # Without oversubscription

        # Compute new resources needed based on oversubcription ratio
        available_oversubscribed = self.get_available()

        missing_oversubscribed   = (request - available_oversubscribed)
        missing_physical = ceil(missing_oversubscribed/self.__get_effective_ratio(with_new_vm=True)) if missing_oversubscribed > 0 else 0
        new_capacity = capacity + missing_physical

        # Check if new_capacity is enough to fullfil VM request without oversubscribing it with itself
        # E.g. a 32vCPU request should be in a pool with 32 physical CPU, no matter what others VM are in it.
        if new_capacity < request:
            missing_physical+= ceil(request-new_capacity)
        return missing_physical

    def get_id(self):
        """Return the oversubscription strategy ID
        ----------

        Returns
        -------
        id : str
           oversubscription id
        """
        return self.ratio

    def __get_effective_ratio(self, with_new_vm : bool = False):
        """Get oversubscription ratio to apply based on if critical size was reached or not
        ----------

        Parameters
        ----------
        with_new_vm : bool (opt)
            If a new VM is to be considered while computing if critical size is reached

        Returns
        -------
        ratio : float
           oversubscription
        """
        count = self.subset.count_consumer()
        if with_new_vm: count+=1
        if count < self.critical_size: # Check if oversubscription critical size is reached
            return 1.0
        return self.ratio

    def __str__(self):
        return 'static oc:' + str(self.ratio)