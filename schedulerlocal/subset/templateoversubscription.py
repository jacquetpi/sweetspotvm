from schedulerlocal.domain.domainentity import DomainEntity
from math import ceil, floor
import os

class TemplateOversubscription(object):
    """
    An oversubscription template is in charge of deducing on which subset belongs VM res
    ...

    Public Methods
    -------
    get_subsets
    """

    def get_subsets_for(self, vm : DomainEntity):
        """For a given VM, get its appropriate subsets ID
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        subsets : list of Tuples
            List of of subsets id. [(subset for res0 : quantity) , (subset for res1 : quantity) ...]
        """
        raise NotImplementedError()

    def get_quantity(self, vm : DomainEntity):
        """For a given VM return resource quantity requested
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        resource : float
            Resource quantity
        """
        raise NotImplementedError()

class TemplateOversubscriptionCpu(TemplateOversubscription):
    """
    An oversubscription template is in charge of deducing on which subset belongs VM cores
    ...

    Public Methods
    -------
    get_subsets_for
    """
    def __init__(self) -> None:
        self.template = [float(oversubscription)for oversubscription in os.getenv('OVSB_TEMPLATE').split(',')]

    def get_subsets_for(self, vm : DomainEntity):
        """For a given VM, get its appropriate subsets ID
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        subsets : list of Tuples
            List of of subsets id. [(subset for core0 : quantity) , (subset for core1 : quantity) ...]
        """
        starting_at = 0
        return [(self.get_from_template(cpuid=cpu),self.get_quantity(vm=vm)) for cpu in range(starting_at, vm.get_cpu()+starting_at)]

    def get_quantity(self, vm : DomainEntity):
        """For a given VM return resource quantity request
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        resource : float
            Resource quantity
        """
        return 1 # Oversubscription is on a per vcpu baseline, no matter what the vm is requesting

    def get_from_template(self, cpuid : int):
        """For a given cpuid, return oversubscription from template
        ----------

        Parameters
        ----------
        cpuid : int
            The CPUID to consider

        Returns
        -------
        resource : float
            Oversubscription ratio
        """
        if cpuid >= len(self.template):
            return self.template[-1]
        return self.template[cpuid]

class TemplateOversubscriptionMem(TemplateOversubscription):
    """
    An oversubscription template is in charge of deducing on which subset belongs VM memory
    ...

    Public Methods
    -------
    get_subsets_for
    """

    def get_subsets_for(self, vm : DomainEntity):
        """For a given VM, get its appropriate subsets ID
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        subsets : List of Tuples
            List of of subsets id. [(subset for mem0 : quantity) , (subset for mem1 : quantity) ...]
        """
        return [(1.0,self.get_quantity(vm=vm))] # Memory is out of scope of this paper

    def get_quantity(self, vm : DomainEntity):
        """For a given VM return resource quantity request
        ----------

        Parameters
        ----------
        vm : DomainEntity
            The VM to consider

        Returns
        -------
        resource : float
            Resource quantity
        """
        return vm.get_mem(as_kb=False) # in MB