from json import loads

class ServerCpu(object):
    """
    A class used to represent a single CPU topology
    ...

    Attributes
    ----------
    cpu_id : int
        ID of CPU
    numa_node : int
        ID of numa node
    sib_smt : list
        List of CPU id sharing the cpu in SMT
    sib_cpu : list
        List of CPU id sharing the numa node
    cache_level :
        Dict of cache level identifier associated to the CPU
    max_freq :
        Max CPU frequency

    Public Methods
    -------
    compute_distance_to_cpu()
        Given another ServerCPU instance, compute the relative distance between them
    Getter/Setter

    """
    def __init__(self, **kwargs):
        req_attributes = ['cpu_id', 'numa_node', 'sib_smt', 'sib_cpu', 'cache_level', 'max_freq']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        self.cpu_time = CpuTime()

    def compute_distance_to_cpu(self, other_cpu, numa_distances : dict):
        """Convert the distance from a given CPU to the current CPU occurence based on Cache level, siblings and numa distances
        ----------

        Parameters
        ----------
        other_cpu : ServerCpu
            Other CPU to compare
        numa_distances : dict
            Numa distances as dict

        Returns
        -------
        distance : int
            Distance as integer
        """
        # Health check
        if self.get_cpu_id() == other_cpu.get_cpu_id(): raise ValueError('Cannot compute distance to itself')
        if len(self.get_cache_level()) != len(other_cpu.get_cache_level()):
            # If heterogenous cache level exists, be careful to distance step incrementation
            raise ValueError('Cannot manage heterogenous cache level between', self.get_cpu_id(), 'and', other_cpu.get_cache_level())

        # Init
        distance = 0
        step = 10

        # Test cache level
        for cache_level, cache_id in self.get_cache_level().items(): 
            distance+=step
            if cache_id == other_cpu.get_cache_level()[cache_level]:
                return distance # Match on given cache

       # Test numa NUMA distance
        distance+= numa_distances[self.get_numa_node()][other_cpu.get_numa_node()]
        return distance

    def get_cpu_id(self):
        """Return unique CPUID
        ----------
        """
        return self.cpu_id

    def get_numa_node(self):
        """Return numa node related to the CPU
        ----------
        """
        return self.numa_node

    def get_sib_smt(self):
        """Return CPUID of siblings SMT cores
        ----------
        """
        return self.sib_smt

    def get_sib_cpu(self):
        """Return CPUID of siblings socket cores
        ----------
        """
        return self.sib_cpu

    def get_hist(self):
        """Return Object keeping track of cpu time counters
        ----------
        """
        return self.cpu_time

    def get_cache_level(self):
        """Return dict of cacheid related to the CPU
        ----------
        """
        return self.cache_level

    def get_max_freq(self):
        """Return core max freq
        ----------
        """
        return self.max_freq

    def __str__(self):
        """Return string representation of the core
        ----------
        """
        return 'cpu' + str(self.get_cpu_id()) +\
            ' ' + str(self.get_max_freq()/1000) + 'Mhz' +\
            ' on numa node ' + str(self.get_numa_node()) +\
            ' with cache level id ' + str(self.get_cache_level())

class CpuTime(object):
    """
    Object keeping track of cpu time counters
    ...
    """
     
    def has_time(self):
        """Return if CPU times were initialised
        ----------
        """
        return hasattr(self, 'idle') and hasattr(self, 'not_idle')

    def set_time(self, idle : int, not_idle : int):
        """Set CPU time
        ----------
        """
        setattr(self, 'idle', idle)
        setattr(self, 'not_idle', not_idle)

    def get_time(self):
        """Return CPU time
        ----------
        """
        return getattr(self, 'idle'), getattr(self, 'not_idle')

    def clear_time(self):
        """Remove registered CPU time
        ----------
        """
        if hasattr(self, 'idle'): delattr(self, 'idle')
        if hasattr(self, 'not_idle'): delattr(self, 'not_idle')

class ServerCpuSet(object):
    """
    A class used to represent CPU topology of a given node
    Proximity between CPU is considered based on a distance node
    ...

    Attributes
    ----------
    numa_distances : dict()
        Dictionary of numa node distances
    cpu_list : list
        List of CPU
    distances : dict()
        List of CPU
    host_count: int
        Count of CPU on host, without consideration on include/exclude list

    Public Methods
    -------
    add_cpu():
        Add a cpu to the considered cpuset
    build_distances():
        Build relative distances of given cpuset
    dump_as_json():
        Dump current state as a json string
    load_from_json():
        load object attributes from json file
    Getter/SetterList of available CPU ordered by their distance
    """

    def __init__(self, **kwargs):
        self.numa_distances = kwargs['numa_distances'] if 'numa_distances' in kwargs else None
        self.cpu_list = kwargs['cpu_list'] if 'cpu_list' in kwargs else list()
        self.distances = kwargs['distances'] if 'distances' in kwargs else dict()
        self.host_count = kwargs['host_count'] if 'host_count' in kwargs else None

    def add_cpu(self, cpu : ServerCpu):
        """Add a ServerCpu object
        ----------

        Parameters
        ----------
        cpu : ServerCpu
            cpu to add
        """
        self.cpu_list.append(cpu)

    def build_distances(self):
        """For each CPU tuple possible in the cpuset, compute the distance based on Cache Level, siblings and numa distances
        Distances are ordered.
        ----------
        """
        if self.numa_distances is None:
            raise ValueError('Numa distances weren\'t previously set')
        self.distances = dict()
        for cpu in self.cpu_list:
            single_cpu_distances = dict()
            others_cpu = list(self.cpu_list)
            others_cpu.remove(cpu)
            for other_cpu in others_cpu:
                single_cpu_distances[other_cpu.get_cpu_id()] = cpu.compute_distance_to_cpu(other_cpu, self.numa_distances)
            # Reorder distances from the closest one to the farthest one 
            self.distances[cpu.get_cpu_id()] = {k:v for k, v in sorted(single_cpu_distances.items(), key=lambda item: item[1])}
        return self

    def load_from_json(self, json : str):
        """Instantiate attributes from a json str
        ----------

        Parameters
        ----------
        json : str
            json str to read

        Returns
        -------
        self : ServerCpuSet
            itself
        """
        raw_object = loads(json)['cpuset']
        self.numa_distances = {int(k):v for k,v in raw_object['numa_distances'].items()}
        self.distances = {int(k):{int(kprime):vprime for kprime,vprime in v.items()} for k,v in raw_object['distances'].items()}
        self.cpu_list = list()
        self.host_count = raw_object['host_count']
        for raw_cpu in raw_object['cpu_list']: self.cpu_list.append(ServerCpu(**raw_cpu))
        return self

    def get_host_count(self):
        """Return Count of CPU on host, without consideration on include/exclude list
        ----------
        """
        return self.host_count

    def get_cpu_list(self):
        """Return CPU list
        ----------
        """
        return self.cpu_list

    def set_cpu_list(self, cpu_list : list):
        """Set CPU list
        ----------
        """
        self.cpu_list = cpu_list

    def get_numa_distances(self):
        """Return numa distances as dict
        ----------
        """
        return self.numa_distances

    def set_numa_distances(self, numa_distances : dict):
        """Set numa distances
        ----------
        """
        self.numa_distances = numa_distances

    def get_distances(self):
        """Return distances (raise an exception if werent previously build with build_distances() method)
        ----------
        """
        if not self.distances: raise ValueError('Distances weren\'t previously build')
        return self.distances

    def get_allowed(self):
        """Return usable CPU count for VMs
        ----------

        Returns
        -------
        count : CPU
            Count of CPU
        """
        return len(self.get_cpu_list())

    def get_distance_between_cpus(self, cpu0 : ServerCpu, cpu1 : ServerCpu):
        """Retrieve the distance between two ServerCpu objects
        ----------

        Parameters
        ----------
        cpu0 : ServerCpu
            The first CPU
        cpu1 : ServerCpu
            The second CPU

        Returns
        -------
        Distance : int
            Distance between two CPUs
        """
        if not self.distances: raise ValueError('Distances weren\'t previously build')
        return self.distances[cpu0.get_cpu_id()][cpu1.get_cpu_id()]