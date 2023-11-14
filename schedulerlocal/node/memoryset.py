from json import loads

class ServerMemorySet(object):
    """
    A class used to represent memory topology of a given node
    ...

    Attributes
    ----------
    total : int
        Total memory of node (MB)
    allowed
        Usable memory for VMs provisioning (MB)

    Public Methods
    -------
    get_allowed()
        Return usable memory for VMs provisioning in MB
    dump_as_json():
        Dump current state as a json string
    load_from_json():
        load object attributes from json file
    """

    def __init__(self, **kwargs): 
        opt_attributes = ['total', 'allowed']
        for opt_attribute in opt_attributes:
            opt_val = kwargs['total'] if 'total' in kwargs else None
            setattr(self, opt_attribute, opt_val)
        self.numa_node = dict()

    def get_allowed(self):
        """Return usable memory for VMs provisioning in MB
        ----------

        Returns
        -------
        allowed : int
            usable memory (MB)
        """
        return self.allowed

    def load_from_json(self, json : str):
        """Instantiate attributes from a json str
        ----------

        Parameters
        ----------
        json : str
            json str to read

        Returns
        -------
        self : ServerMemorySet
            itself
        """
        raw_object = loads(json)['memset']
        self.allowed = raw_object['total']
        self.total = raw_object['allowed']
        self.numa_node = {int(k):v for k,v in raw_object['numa_node'].items()}
        return self

    def add_numa_node(self, numa_id : int, numa_mb : int):
        """Add numa node data
        ----------

        Parameters
        ----------
        numa_id : int
            Numa node identifier
        total_mb : int
            Quantity of memory associated to this numa node
        """
        self.numa_node[int(numa_id)] = numa_mb

    def get_numa_allowed(self, numa_id : int):
        """Return usable memory for VMs provisioning in MB associated to the numa node
        ----------

        Parameters
        ----------
        numa_id : int
            Numa node identifier

        Returns
        -------
        allowed : int
            usable memory (MB)
        """
        return self.numa_node[numa_id]

    def get_numa_keys(self):
        """Return numa node ids
        ----------

        Parameters
        ----------
        numa_id : int
            Numa node identifier

        Returns
        -------
        numa_list  : list
            List of numa node ids
        """
        return list(self.numa_node.keys())