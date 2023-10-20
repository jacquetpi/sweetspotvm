from collections import defaultdict

class DomainEntity:
    """
    Data Access Object (DAO) representing a domain (i.e. a VM)
    ...

    Attributes
    ----------
    uuid : str
        VM identifier
    name : str
        VM name
    cpu : int
        CPU allocation
    cpu_pin : list
        list of vCPU pinned situation
    cpu_ratio : float
        CPU oversubscription ratio

    Public Methods
    -------
    Getter/Setter
    """

    def __init__(self, **kwargs):
        req_attributes = ['name', 'mem', 'cpu', 'cpu_ratio']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        opt_attributes = ['uuid', 'cpu_pin', 'qcow2']
        for opt_attribute in opt_attributes:
            if opt_attribute in kwargs:
                setattr(self, opt_attribute, kwargs[opt_attribute])
            else: setattr(self, opt_attribute, None)
        self.being_destroyed = False

    def get_uuid(self):
        """Return VM uuid
        ----------
        """
        return self.uuid

    def is_being_destroyed(self):
        """Return VM is_being_destroyed attribute
        ----------
        """
        return self.being_destroyed

    def set_being_destroyed(self, being_destroyed : bool):
        """Set VM is_being_destroyed attribute
        ----------
        """
        self.being_destroyed=being_destroyed

    def set_uuid(self, uuid :str):
        """set VM uuid
        ----------
        """
        self.uuid = uuid

    def is_deployed(self):
        """Return if VM is deployed
        ----------
        """
        return self.uuid != None

    def get_name(self):
        """Return VM name
        ----------
        """
        return self.name

    def get_mem(self, as_kb : bool = True):
        """Return mem allocation
        ----------
        """
        if as_kb: return self.mem
        return int(self.mem/1024) # as_mb

    def get_cpu(self):
        """Return CPU allocation
        ----------
        """
        return self.cpu

    def get_cpu_pin(self):
        """Return CPU pin situation
        ----------
        """
        return self.cpu_pin

    def set_cpu_pin(self, template_pin : tuple):
        """Set CPU pin situation based on template
        ----------
        """
        self.cpu_pin = [template_pin for vcpu in range(self.get_cpu())]

    def get_cpu_pin_aggregated(self):
        """Return a dict specifying for each cpuid if at least one vCPU is pinned to it (boolean value)
        ----------
        """
        aggregated_vm_cpu_pin = defaultdict(lambda: False)
        for vcpu_pin in self.get_cpu_pin(): 
            for cpu, is_pinned in enumerate(vcpu_pin): 
                aggregated_vm_cpu_pin[cpu] = (aggregated_vm_cpu_pin[cpu] or is_pinned)
        return aggregated_vm_cpu_pin

    def get_cpu_ratio(self):
        """Return CPU ratio
        ----------
        """
        return self.cpu_ratio

    def get_qcow2(self):
        """Return disk location on host
        ----------
        """
        return self.qcow2

    def has_time(self):
        """Return if CPU times were initialised
        ----------
        """
        return hasattr(self, 'epoch_ns') and hasattr(self, 'total') and hasattr(self, 'system') and hasattr(self, 'user')

    def set_time(self, epoch_ns : int, total : int, system: int, user : int):
        """Set CPU time
        ----------
        """
        setattr(self, 'epoch_ns', epoch_ns)
        setattr(self, 'total', total)
        setattr(self, 'system', system)
        setattr(self, 'user', user)

    def get_time(self):
        """Return CPU time
        ----------
        """
        return getattr(self, 'epoch_ns'), getattr(self, 'total'), getattr(self, 'system'), getattr(self, 'user')

    def clear_time(self):
        """Remove registered CPU time
        ----------
        """
        if hasattr(self, 'epoch_ns'): delattr(self, 'epoch_ns')
        if hasattr(self, 'total'): delattr(self, 'total')
        if hasattr(self, 'system'): delattr(self, 'system')
        if hasattr(self, 'user'): delattr(self, 'user')

    def __eq__(self, other): 
        if not isinstance(other, DomainEntity): return False
        if id(self) is id(other): return True
        if self.uuid == other.uuid: return True
        if (self.name == other.name) and (self.cpu == other.cpu) and (self.mem) == (other.mem): return True
        return False

    def __str__(self):
        """Convert state to string
        ----------
        """
        return 'vm ' + self.get_name() + ' ' + str(self.get_cpu()) + 'vCPU ' + str(self.get_mem(as_kb=False)) + 'MB with oc ' + str(self.get_cpu_ratio()) + '\n'