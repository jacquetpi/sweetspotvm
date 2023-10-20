import re
from schedulerlocal.node.memoryset import ServerMemorySet

class MemoryExplorer:
    """
    A class used to retrieve Memory Information
    ...

    Public Methods
    -------
    build_memoryset():
       Build a MemorySet object from linux filesystem data
    """

    def __init__(self, **kwargs):
        self.fs_meminfo = '/proc/meminfo'
        self.private_mb = kwargs['private_mb'] if 'private_mb' in kwargs else 0

    def build_memoryset(self):
        with open(self.fs_meminfo, 'r') as f:
            meminfo = f.read()
        total_found = re.search('^MemTotal:\s+(\d+)', meminfo)
        if not total_found: raise ValueError('Error while parsing', self.fs_meminfo)
        total_kb = int(total_found.groups()[0])
        total_mb = int(total_kb/1024)
        allowed_mb = total_mb - self.private_mb
        return ServerMemorySet(total=total_mb, allowed_mb=allowed_mb)

    def get_usage_of(self,  server_mem_list : list):
        """Return the Memory usage of a given ServerMemory object list
        /!\ Multiple MemSubset is not supported. We just report host memory usage for now
        ----------

        Parameters
        ----------
        server_mem_list : list
            ServerMem object list

        Returns
        -------
        mem_usage : int
            Usage as [0;n] n being the number of element in server_mem_list
        """
        # Multiple MemSubset is not supported
        return self.get_usage_global()

    def get_usage_global(self):
        """Return  host memory usage
        ----------

        Returns
        -------
        mem_usage : int
            Usage as [0;1]
        """
        with open(self.fs_meminfo, 'r') as f:
            meminfo = f.readlines()
        
        total_found = re.search('^MemTotal:\s+(\d+)', meminfo[0])
        if not total_found: raise ValueError('Error while parsing', self.fs_meminfo)
        total_kb = int(total_found.groups()[0])

        available_found = re.search('^MemAvailable:\s+(\d+)', meminfo[2])
        if not available_found: raise ValueError('Error while parsing', self.fs_meminfo)
        available_kb = int(available_found.groups()[0])

        mem_usage = (total_kb-available_kb)/total_kb
        return mem_usage
