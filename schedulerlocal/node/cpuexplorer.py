import re
from os import listdir, sysconf
from os.path import isfile, join, exists
from schedulerlocal.node.cpuset import ServerCpu, CpuTime, ServerCpuSet

class CpuExplorer:
    """
    A class used to retrieve CPU data from Linux FS
    ...

    Attributes
    ----------
    to_include : list (optional)
        If specified, only core of this list will be considered (default to all)
    to_exclude : list (optional)
        If specified, core from this list will be excluded (default to none)

    Public Methods
    -------
    build_cpuset():
       Build a ServerCpuSet object from linux filesystem data
    get_cpu_usage():
        Return the CPU usage of a given ServerCpu Object
    """

    def __init__(self, **kwargs):
        attributes = ['to_include', 'to_exclude']
        for attribute in attributes:
            setattr(self, attribute, kwargs[attribute] if attribute in kwargs else list())
        self.fs_cpu           = '/sys/devices/system/cpu/'
        self.fs_cpu_topology  = '/topology'
        self.fs_cpu_cache     = '/cache/index'
        self.fs_cpu_maxfreq   = '/cpufreq/cpuinfo_max_freq'
        self.fs_numa          = '/sys/devices/system/node/'
        self.fs_numa_distance = '/distance'
        self.fs_stat          = '/proc/stat'
        # From https://www.kernel.org/doc/Documentation/filesystems/proc.txt
        self.fs_stats_keys         = {'cpuid':0, 'user':1, 'nice':2 , 'system':3, 'idle':4, 'iowait':5, 'irq':6, 'softirq':7, 'steal':8, 'guest':9, 'guest_nice':10}
        self.fs_stats_idle         = ['idle', 'iowait']
        self.fs_stats_not_idle     = ['user', 'nice', 'system', 'irq', 'softirq', 'steal']
        self.global_cpu_time       = CpuTime()

    def build_cpuset(self):
        """Build a ServerCpuSet object from linux filesystem data
        ----------

        Returns
        -------
        cpuset : ServerCpuSet
            Local Cpuset
        """
        cpu_count, cpu_list_conform = self.__retrieve_cpu_list()
        cpuset = ServerCpuSet(host_count=cpu_count)
        for cpu in cpu_list_conform: cpuset.add_cpu(self.__read_cpu(cpu, cpu_list_conform))
        cpuset.set_numa_distances(self.__read_numa_distance())
        return cpuset.build_distances()

    def get_usage_of(self, server_cpu_list : list):
        """Return the CPU usage of a given ServerCpu object list. None if unable to compute it (as delta values are needed=
        ----------

        Parameters
        ----------
        server_cpu_list : list
            ServerCpu object list

        Returns
        -------
        cpu_usage : float
            Usage as [0;n] n being the number of element in server_cpu_list
        """
        hist_by_cpu = {'cpu'  + str(server_cpu.get_cpu_id()):server_cpu.get_hist() for server_cpu in server_cpu_list}
        cumulated_cpu_usage = 0
        with open(self.fs_stat, 'r') as f:
            lines = f.readlines()

        for line in lines:
            
            split = line.split(' ')
            if not split[self.fs_stats_keys['cpuid']].startswith('cpu'): break
            if split[self.fs_stats_keys['cpuid']] not in hist_by_cpu.keys(): continue
            
            hist_object = hist_by_cpu[split[self.fs_stats_keys['cpuid']]]
            cpu_usage = self.__get_usage_of_line(split=split, hist_object=hist_object)
        
            # Add usage to cumulated value
            if cumulated_cpu_usage != None and cpu_usage != None:
                cumulated_cpu_usage+=cpu_usage
            else: cumulated_cpu_usage = None # Do not break to compute others initializing values

        return cumulated_cpu_usage

    def get_usage_global(self):
        """Return host CPU usage. None if unable to compute it (as delta values are needed=
        ----------

        Parameters
        ----------
        server_cpu_list : list
            ServerCpu object list

        Returns
        -------
        cpu_usage : float
            Usage as [0;n] n being the number of element in server_cpu_list
        """
        with open(self.fs_stat, 'r') as f:
            split = f.readlines()[0].split(' ')
            split.remove('')
        return self.__get_usage_of_line(split=split, hist_object=self.global_cpu_time)

    def __get_usage_of_line(self, split : list, hist_object : object):
        """Based on a /proc/stat splitted CPU line and an object having previous value, compute usage as delta
        None if not able to compute the delta
        ----------

        Parameters
        ----------
        split : list
            splitted CPU line from /proc/stat file
        hist_object : object
            Object having previous CPU time
            
        Returns
        -------
        cpu_usage : float
            Usage as [0;1]
        """
        idle          = sum([ int(split[self.fs_stats_keys[idle_key]])     for idle_key     in self.fs_stats_idle])
        not_idle      = sum([ int(split[self.fs_stats_keys[not_idle_key]]) for not_idle_key in self.fs_stats_not_idle])

        # Compute delta
        cpu_usage  = None
        if hist_object.has_time():
            prev_idle, prev_not_idle = hist_object.get_time()
            delta_idle     = idle - prev_idle
            delta_total    = (idle + not_idle) - (prev_idle + prev_not_idle)
            cpu_usage      = (delta_total-delta_idle)/delta_total
        hist_object.set_time(idle=idle, not_idle=not_idle)
        return cpu_usage

    def __retrieve_cpu_list(self):
        """Retrieve the list of cpu id conform to to_include and to_exclude attributes
        ----------

        Returns
        -------
        cpu_found : int
            Number of CPU on host
        cpu_conform : str
            list of CPU
        """
        regex = '^cpu[0-9]+$'
        cpu_found = [int(re.sub("[^0-9]", '', f)) for f in listdir(self.fs_cpu) if not isfile(join(self.fs_cpu_topology, f)) and re.match(regex, f)]
        cpu_conform = [core for core in cpu_found if (core not in self.to_exclude) and (not self.to_include or core in self.to_include)]
        cpu_conform.sort()
        return len(cpu_found), cpu_conform

    def __read_cpu(self, cpu : int, conform_cpu_list : list):
        """Build a ServerCpu object from specified cpu id using Linux filesystem data
        ----------

        Parameters
        ----------
        cpu : int
            ID of targeted CPU
        conform_cpu_list : list
            List of others conform CPU to build sibling lists

        Returns
        -------
        cpu : ServerCpu
            ServerCpu object describing targeted CPU
        """
        conform_cpu_list_copy = list(set(conform_cpu_list))
        if cpu in conform_cpu_list_copy: conform_cpu_list_copy.remove(cpu)

        numa_node, sib_smt, sib_cpu = self.__read_cpu_topology(cpu, conform_cpu_list_copy)
        cache_level = self.__read_cpu_cache(cpu)
        max_freq = self.__read_cpu_maxfreq(cpu)

        return ServerCpu(cpu_id=cpu,\
            numa_node=numa_node, sib_smt=sib_smt, sib_cpu=sib_cpu,\
            cache_level=cache_level,\
            max_freq=max_freq)

    def __read_cpu_topology(self, cpu : int, conform_cpu_list : list):
        """Retrieve topology related data of specified cpu from Linux filesystem
        ----------

        Parameters
        ----------
        cpu : int
            ID of targeted CPU
        conform_cpu_list : list
            List of others conform CPU to build sibling lists

        Returns
        -------
        numa_id : int
            Id of numa node hosting the CPU
        sib_smt_list : list
            List of CPU id being SMT sibling to the CPU
        sib_cpu_list : list
            List of CPU id being socket sibling to the CPU
        """
        topology_folder = self.fs_cpu + 'cpu' + str(cpu) + self.fs_cpu_topology
        with open(topology_folder + '/physical_package_id', 'r') as f:
            numa_id = int(f.read())
        with open(topology_folder + '/thread_siblings_list', 'r') as f:
            sib_smt_list = [sibling_smt for sibling_smt in self.__convert_text_to_list(f.read()) if (sibling_smt != cpu) and sibling_smt in conform_cpu_list]
        with open(topology_folder + '/core_siblings_list', 'r') as f:
            sib_cpu_list = [sibling_cpu for sibling_cpu in self.__convert_text_to_list(f.read()) if (sibling_cpu != cpu) and sibling_cpu in conform_cpu_list]
        return numa_id, sib_smt_list, sib_cpu_list

    def __read_cpu_cache(self, cpu : int):
        """Retrieve cache related data of specified cpu from Linux filesystem
        ----------

        Parameters
        ----------
        cpu : int
            ID of targeted CPU

        Returns
        -------
        cache_dict : dict
            Dictionary of cache level (as key) specifying the related cache unique identifier
        """
        cache_level = 0
        cache_dict = dict()
        while(True):
            cache_file = self.fs_cpu + 'cpu' + str(cpu) + self.fs_cpu_cache + str(cache_level) + '/id'
            if not exists(cache_file): break
            with open(cache_file , 'r') as f:
                cache_dict[cache_level] = int(f.read())
            cache_level+=1
        return cache_dict

    def __read_cpu_maxfreq(self, cpu : int):
        """Retrieve CPU max frequency of specified cpu from Linux filesystem
        ----------

        Parameters
        ----------
        cpu : int
            ID of targeted CPU

        Returns
        -------
        maxfreq : int
            Max frequency
        """
        maxfreq_file = self.fs_cpu + 'cpu' + str(cpu) + self.fs_cpu_maxfreq
        with open(maxfreq_file , 'r') as f:
            maxfreq = int(f.read())
        return maxfreq

    def __read_numa_distance(self):
        """Retrieve distances of local numa node
        ----------

        Parameters
        ----------
        cpu : int
            ID of targeted CPU

        Returns
        -------
        numa_dict : dict
            Dictionary of numa id (as key) specifying the distance to others numa node
        """
        numa_index = 0
        numa_dict = dict()
        while True:
            fs_distance = self.fs_numa + 'node' + str(numa_index) + self.fs_numa_distance
            if not exists(fs_distance): break
            with open(fs_distance, 'r') as f:
                numa_dict[numa_index] = [int(element) for element in f.read().replace('\n', '').split(' ')]
            numa_index+=1
        return numa_dict

    def __convert_text_to_list(self, text : str):
        """Convert a text as observed in /sys/device fs to a list of integers
        ----------

        Parameters
        ----------
        text : str
            Text to convert

        Returns
        -------
        converted_list : list
            List of integers
        """
        text_to_convert = text.replace('\n', '')
        if ',' in text_to_convert:
            result_list = list()
            for element in text_to_convert.split(','): result_list.extend(self.__convert_text_to_list(element))
            return result_list
        elif '-' in text_to_convert:
            left_member = int(text_to_convert[:text_to_convert.find('-')])
            right_member = int(text_to_convert[text_to_convert.find('-')+1:])
            return list(range(left_member, right_member+1))
        else:
            return [int(text_to_convert)]