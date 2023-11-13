import re
from schedulerlocal.node.memoryset import ServerMemorySet
from os import listdir
from os.path import isfile, join

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
        self.fs_meminfo  = '/proc/meminfo'
        self.fs_numainfo = '/sys/devices/system/node/'
        self.fs_numafile = '/meminfo'
        self.private_mb  = kwargs['private_mb'] if 'private_mb' in kwargs else 0

    def build_memoryset(self):
        # Retrieve global data
        with open(self.fs_meminfo, 'r') as f:
            global_info = f.read()
        total_mb = self.read_total(global_info)
        allowed_mb = total_mb - self.private_mb
        memoryset =  ServerMemorySet(total=total_mb, allowed_mb=allowed_mb)

        # Retrieve per numa data
        regex = '^node[0-9]+$'
        numa_locations = [f for f in listdir(self.fs_numainfo) if re.match(regex, f)]
        for location in numa_locations: 
            numa_path = self.fs_numainfo + location + self.fs_numafile
            numa_id = re.sub('[^0-9]', '', location)
            with open(numa_path, 'r') as f:
                memoryset.add_numa_node(numa_id=numa_id, numa_mb=self.read_total(f.read(), numa_format=True))

        return memoryset

    def read_total(self, data_as_string : str, numa_format : bool = False):
        total_line = data_as_string.split('\n')[0]
        splitted_line = re.sub(' +', ' ', total_line).split(' ')
        if numa_format:
            total_kb = int(splitted_line[3])
        else:
            total_kb = int(splitted_line[1])
        return int(total_kb/1024)

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
