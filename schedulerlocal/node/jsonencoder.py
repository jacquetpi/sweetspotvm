from json import JSONEncoder
from schedulerlocal.node.memoryset import ServerMemorySet
from schedulerlocal.node.cpuset import ServerCpuSet, ServerCpu

class ServerCpuSetEncoder(JSONEncoder):
    """
    Class to specify on to convert ServerCpuSet to JSON
    ...

    Public MethodsServerCpuSet
        json conversion
    """

    def default(self, o):
        """Implements Conversion strategy
        ----------

        Parameters
        ----------
        o : object
            object to convert
        """
        if type(o) is not ServerCpuSet:
            return
        as_dict = dict(o.__dict__)
        as_dict['cpu_list'] = [self.convert_cpu_to_dict(cpu) for cpu in o.__dict__['cpu_list']]
        return as_dict

    @staticmethod
    def convert_cpu_to_dict(cpu : ServerCpu):
        cpu_dict = dict(cpu.__dict__ )
        del cpu_dict['cpu_time']
        return cpu_dict

class ServerMemorySetEncoder(JSONEncoder):
    """
    Class to specify on to convert MemoryCpuSet to JSON
    ...

    Public MethodsServerCpuSet
        json conversion
    """

    def default(self, o):
        """Implements Conversion strategy
        ----------

        Parameters
        ----------
        o : object
            object to convert
        """
        if type(o) is not ServerMemorySet:
            return
        as_dict = dict(o.__dict__)
        return as_dict

class SubsetEncoder(JSONEncoder):
    """
    Class to specify on to convert Subset to JSON
    ...

    Public MethodsServerCpuSet
        json conversion
    """

    def default(self, o):
        """Implements Conversion strategy
        ----------

        Parameters
        ----------
        o : object
            object to convert
        """
        from schedulerlocal.subset.subset import CpuSubset, MemSubset # avoid circular import, quite ugly :(
        if (type(o) is not CpuSubset) and (type(o) is not MemSubset):
            return

        as_dict = {'res_list':None, 'consumer_list':None}
        as_dict['consumer_list'] = [vm.__dict__ for vm in o.get_consumers()]
        if type(o) is CpuSubset:
            as_dict['res_list'] = [ServerCpuSetEncoder.convert_cpu_to_dict(cpu) for cpu in o.get_res()]
        elif type(o) is MemSubset:
            as_dict['res_list'] = o.get_res()
        else:
            raise NotImplementedError('Unknow subset type for json serialization')
        return as_dict

class GlobalEncoder(JSONEncoder):
    """
    Class to specify on to convert ServerCpuSet to JSON
    ...

    Public MethodsServerCpuSet
        json conversion
    """

    def default(self, o, *args, **kwargs):
        """Implements Conversion strategy
        ----------

        Parameters
        ----------
        o : object
            object to convert
        """
        from schedulerlocal.subset.subset import CpuSubset, CpuElasticSubset, MemSubset # avoid circular import, quite ugly :(
        if type(o) is ServerCpuSet:
            return ServerCpuSetEncoder(*args, **kwargs).default(o)
        elif type(o) is ServerMemorySet:
            return ServerMemorySetEncoder(*args, **kwargs).default(o)
        elif (type(o) is CpuSubset) or (type(o) is CpuElasticSubset) or (type(o) is MemSubset):
            return SubsetEncoder(*args, **kwargs).default(o)
        elif type(o) is dict:
            return dict(o.__dict__)
        else:
            raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')