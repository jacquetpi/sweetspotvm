from collections import defaultdict
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv
import os, json
from schedulerlocal.node.cpuexplorer import CpuExplorer
from schedulerlocal.node.memoryexplorer import MemoryExplorer
from schedulerlocal.domain.domainentity import DomainEntity

class DataEndpoint(object):
    """
    An Endpoint is a class charged to store or retrieve subset data 
    Abstract class
    ...

    Public Methods
    -------
    todo()
        todo
    """
    def load_subset(self, timestamp : int, subset):
        """Return subset resources usage. Must be reimplemented
        ----------
        """
        raise NotImplementedError()

    def load_global(self, timestamp : int, manager):
        """Return subset resources usage. Must be reimplemented
        ----------
        """
        raise NotImplementedError()

    def is_live(self):
        """Return a boolean value based on if values are loaded from a live system
        ----------

        Return
        ----------
        Live : boolean
            True if system is live
        """
        return False

    @staticmethod
    def record(tmp : int, rec : str, res : str, val : float, config : float,\
            subset : str = None,\
            sb_oc : str = None, sb_unused : float = None, sb_dsc : str = None,\
            vm_uuid : str = None, vm_cmn : str = None):
        """Return data as structured dict
        ----------

        Parameters
        ----------
        tmp : int 
            timestamp of data
        rec : str
            type of record. Possible options are vm, subset or global
        res :str
            Resource considered (e.g. cpu/mem...)
        val : float
            Value registered
        config : float
            Configuration Capacity
        subset : str (default to None)
            If applicable, subset id (VM/subset)
        sb_oc: str (default to None)
            If applicable, oversubscription (VM/subset)
        sb_unused : float
            If applicable, subset current unused res (subset)
        sb_dsc : str
            If applicable, subset json description (subset) 
        vm_uuid : str (default to None)
            If applicable, UUID of consumer (vm)
        vm_cmn : str (default to None)
            If applicable, common name of consumer (vm)

        Return
        ----------
        data : dict
            Data as dict
        """
        if rec == 'subset':
            if (subset == None) or (sb_oc == None) or (sb_unused == None) or (sb_dsc == None):
                raise ValueError('Missing requirements parameters for subset record')
        elif rec == 'vm':
            if (subset == None) or (vm_uuid == None) or (vm_cmn == None) or (sb_oc == None):
                raise ValueError('Missing requirements parameters for vm record')
        elif rec != 'global':
            raise ValueError('Unknow record' + rec)
        return {'tmp':tmp, 'rec': rec, 'res':res, 'val':val, 'config':config,\
            'subset':subset,\
            'vm_uuid':vm_uuid, 'vm_cmn':vm_cmn,\
            'sb_oc':sb_oc, 'sb_unused':sb_unused, 'sb_dsc':sb_dsc}

    def get_record_keys(self):
        # Create a fake record
        rec = self.record(tmp=0,rec='global',res='res',val=0.0,config=0.0)
        return list(rec.keys())

    def store(self, record : dict):
        """Return available resources. Must be reimplemented
        ----------
        """
        raise NotImplementedError()

class DataEndpointLive(DataEndpoint):
    """
    A live endpoint load data from the live system. It cannot store data
    ...

    Public Methods
    -------
    load()
        load resource usage and vm usage
    """
        
    def load_subset(self, timestamp : int, subset):
        """Return subset resources usage. Must be reimplemented
        ----------
        """
        # Use subset explorer
        subset_usage = subset.get_current_resources_usage()
        # Use libvirt connector
        vm_usage = subset.get_current_consumers_usage()
        return subset_usage, vm_usage

    def load_global(self, timestamp : int, manager):
        # Use subset explorer
        return manager.get_current_resources_usage()

    def is_live(self):
        """Return a boolean value based on if values are loaded from a live system
        ----------

        Return
        ----------
        Live : boolean
            True if system is live
        """
        return True

class DataEndpointCSV(DataEndpoint):
    """
    A CSV endpoint store and load data from a CSV file
    ...

    Public Methods
    -------
    store()
        store
    """
    def __init__(self, **kwargs):
        req_attributes = ['input_file', 'output_file']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        self.keys = self.get_record_keys()
        self.separator = '\t'
        self.new_line  = '\n'
        self.header = ''.join(self.separator + str(key) for key in self.keys)
        self.header = self.header.replace(self.separator, '', 1) # remove first separator
        if self.input_file is not None: self.__load_input_csv()
        if self.output_file is not None:
            with open(self.output_file, 'w') as f: f.write(self.header + self.new_line)

    def __load_input_csv(self):
        """Load a CSV file to store its data in dicts
        ----------
        """
        print('Loading CSV file. Warning: this step is time consuming')
        if self.input_file is None: raise ValueError('No CSV input file specified')
        self.input_timestamp = set()
        self.input_global  = {'cpu' : dict(), 'mem' : dict()}
        self.input_subset  = {'cpu' : dict(), 'mem' : dict()}
        self.input_vm      = {'cpu' : dict(), 'mem' : dict()}
        self.input_vm_spec = dict()
        with open(self.input_file) as fp:
            for i, line in enumerate(fp):
                if i == 0: continue
                line_as_list = line.split(self.separator)
                timestamp = int(line_as_list[self.keys.index('tmp')])
                record = line_as_list[self.keys.index('rec')]
                resource = line_as_list[self.keys.index('res')]
                value = float(line_as_list[self.keys.index('val')]) if line_as_list[self.keys.index('val')] != 'None' else None
                self.input_timestamp.add(timestamp)
                if   record == 'global': self.input_global[resource][timestamp] = value
                elif record == 'subset':
                    subset_id = line_as_list[self.keys.index('subset')]
                    if subset_id not in self.input_subset[resource]: self.input_subset[resource][subset_id] = dict()
                    self.input_subset[resource][subset_id][timestamp] = value
                    # also initialize vm list associate to subset
                    if subset_id not in self.input_vm[resource]: self.input_vm[resource][subset_id] = dict()
                    if timestamp not in self.input_vm[resource][subset_id]: self.input_vm[resource][subset_id][timestamp] = list()
                elif record == 'vm':
                    subset_id = line_as_list[self.keys.index('subset')]
                    uuid   = line_as_list[self.keys.index('vm_uuid')]
                    name   = line_as_list[self.keys.index('vm_cmn')]
                    oc     = line_as_list[self.keys.index('sb_oc')]
                    config = line_as_list[self.keys.index('config')]
                    # First, manage reports
                    if subset_id not in self.input_vm[resource]: self.input_vm[resource][subset_id] = dict()
                    if timestamp not in self.input_vm[resource][subset_id]: self.input_vm[resource][subset_id][timestamp] = list()
                    self.input_vm[resource][subset_id][timestamp].append((uuid, value))
                    # Second, manage known specs
                    if uuid not in self.input_vm_spec: self.input_vm_spec[uuid] = dict()
                    if resource not in self.input_vm_spec[uuid]: self.input_vm_spec[uuid][resource] = dict()
                    self.input_vm_spec[uuid]['name']   = name
                    if resource == 'cpu': 
                        self.input_vm_spec[uuid]['cpu_r']  = oc
                        # Third manage deployment and departure data (on cpu only for unicity)
                        if ('tmp_first' not in self.input_vm_spec[uuid]): self.input_vm_spec[uuid]['tmp_first'] = timestamp
                        self.input_vm_spec[uuid]['tmp_last'] = timestamp
                    self.input_vm_spec[uuid][resource] = config
                    
                else:
                    raise ValueError('Unknow record while loading trace', record)
        print('Loading completed')

    def load_subset(self, timestamp : int, subset):
        """Return subset resources usage
        ----------
        """
        if self.input_file is None: raise ValueError('No CSV input file specified')
        subset_usage = self.input_subset[subset.get_res_name()]['subset-' + str(subset.get_oversubscription_id())][timestamp]
        vm_usage = dict()
        for vm_line in self.input_vm[subset.get_res_name()]['subset-' + str(subset.get_oversubscription_id())][timestamp]:
            uuid, value = vm_line
            vm_usage[uuid] = (self.__get_vm_from_uuid(uuid), value)
        return subset_usage, vm_usage

    def load_global(self, timestamp : int, manager):
        """Return global resources usage
        ----------
        """
        if self.input_file is None: raise ValueError('No CSV input file specified')
        return self.input_global[manager.get_res_name()][timestamp]

    def store(self, record : dict):
        if self.output_file is None: raise ValueError('No CSV output file specified')
        line = ''.join([self.separator + str(record[key]) for key in self.keys])
        line = line.replace(self.separator, '', 1) # remove first separator
        with open(self.output_file, 'a') as f: 
            f.write(line + self.new_line)

    def get_timestamp_list(self):
        """Return List of timestamps
        ----------
        """
        if not hasattr(self, 'input_timestamp'): raise ValueError('List of timestamp is only available when loaded from a CSV file')
        timestamp_list = list(self.input_timestamp)
        timestamp_list.sort()
        return timestamp_list

    def get_deployed_vm_on(self, timestamp):
        """Return deployed vm on given timestamp
        ----------
        """
        newly_deployed_vm = list()
        for uuid, specs in self.input_vm_spec.items():
            if specs['tmp_first'] == timestamp:
                vm = self.__get_vm_from_uuid(uuid)
                if vm != None: newly_deployed_vm.append(vm)
        return newly_deployed_vm

    def get_destroyed_vm_on(self, timestamp):
        """Return destroyed vm on given timestamp
        ----------
        """
        # We need to identify VMs destroyed before this timestamp (i.e having a last_seen timestamp being the one right before the parameter)
        timestamp_list = self.get_timestamp_list()
        last_seen_index= timestamp_list.index(timestamp) - 1
        if last_seen_index<0: last_seen_index=0
        newly_removed_vm = list()
        for uuid, specs in self.input_vm_spec.items():
            if specs['tmp_last'] == timestamp_list[last_seen_index]:
                vm = self.__get_vm_from_uuid(uuid)
                if vm != None: newly_removed_vm.append(vm)
        return newly_removed_vm

    def __get_vm_from_uuid(self, uuid : str):
        """Return DomainEntity object based on uuid usign known specs
        ----------
        """
        if ('cpu' not in self.input_vm_spec[uuid]) or ('mem' not in self.input_vm_spec[uuid]): return None
        return DomainEntity(name=self.input_vm_spec[uuid]['name'], mem=int(self.input_vm_spec[uuid]['mem']),\
                cpu=int(self.input_vm_spec[uuid]['cpu']), cpu_ratio=float(self.input_vm_spec[uuid]['cpu_r']), uuid=uuid)


class DataEndpointInfluxDB(DataEndpoint):
    """
    An InfluxDB endpoint store and load data from InfluxDB
    ...

    Public Methods
    -------
    todo()
        todo
    """

    def __init__(self, **kwargs):
        load_dotenv()
        self.url    =  os.getenv('INFLUXDB_URL')
        self.token  =  os.getenv('INFLUXDB_TOKEN')
        self.org    =  os.getenv('INFLUXDB_ORG')
        self.bucket =  os.getenv('INFLUXDB_BUCKET')
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            self.query_api = self.client.query_api()
        except Exception as ex:
            print('An exception occured while trying to connect to InfluxDB, double check your parameters:')
            print('url:', self.url, 'org:', self.org, 'token: [hidden]')
            print('Full stack trace is:\n')
            raise ex

    def load_subset(self, timestamp : int, subset):
        """TODO
        ----------
        """
        end = timestamp + 1000
        query = ' from(bucket:"' + self.bucket + '")\
        |> range(start: ' + str(timestamp) + ', stop: ' + str(end) + ')\
        |> filter(fn: (r) => r["_measurement"] == "domain")\
        |> filter(fn: (r) => r["url"] == "' + self.model_node_name + '")'

        result = self.query_api.query(org=self.org, query=query)
        domains_data = defaultdict(lambda: defaultdict(list))

        for table in result:
            for record in table.records:
                domain_name = record.__getitem__('domain')
                timestamp = (record.get_time()).timestamp()
                if timestamp not in domains_data[domain_name]["time"]:
                    domains_data[domain_name]["time"].append(timestamp)
                domains_data[domain_name][record.get_field()].append(record.get_value())
        raise NotImplementedError()

    def store(self, record : dict):
        """TODO
        ----------
        """
        raise NotImplementedError()

class DataEndpointJson(DataEndpoint):
    """
    A Json endpoint store and load data from a json file
    ...

    Public Methods
    -------
    todo()
        todo
    """

    def __init__(self, **kwargs):
        req_attributes = ['file']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        
        with open(self.input_file, 'r') as f: 
            self.input_data = json.load(f)
        self.output_data = dict()

    def load_subset(self, timestamp : int, subset):
        """TODO
        ----------
        """
        raise NotImplementedError()

    def load_global(self, timestamp : int, manager):
        """TODO
        ----------
        """
        raise NotImplementedError()

    def store(self, record : dict):
        """TODO
        ----------
        """
        raise NotImplementedError()

    def __del__(self):
        """Before destroying object, dump written data
        ----------
        """
        print("JsonEndpoint: dumping data to", self.output_file)
        with open(self.output_file, 'w') as f: 
            f.write(json.dumps(self.output_data))