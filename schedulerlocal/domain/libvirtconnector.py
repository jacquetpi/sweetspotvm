import libvirt, time
from schedulerlocal.domain.libvirtxmlmodifier import xmlDomainNuma, xmlDomainMetaData, xmlDomainCputune
from schedulerlocal.domain.domainentity import DomainEntity

class LibvirtConnector(object):
    """
    A class used as an interface with libvirt API
    ...

    Attributes
    ----------
    url : str
        hypervisor url

    """
    def __init__(self, **kwargs):
        req_attributes = ['url', 'loc', 'machine']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        # Connect to libvirt url    
        self.conn = libvirt.open(self.url)
        if not self.conn:
            raise SystemExit('Failed to open connection to ' + self.url)
        self.cache_entity = dict()

        with open('static/template-vm.xml', 'r') as f: self.template_vm = f.read()

    def get_vm_alive(self):
        """Retrieve list of VM being running currently as libvirt object
        ----------

        Returns
        -------
        vm_alive : list
            list of virDomain
        """
        res = list()
        for domain_id in self.conn.listDomainsID():
            try:
                virDomain = self.conn.lookupByID(domain_id)
                res.append(virDomain)
            except libvirt.libvirtError as ex:  # VM is not alived anymore
                pass
        return res

    def get_vm_alive_as_entity(self):
        """Retrieve list of VM being running currently as DomainEntity object
        ----------

        Returns
        -------
        vm_alive : list
            list of DomainEntity
        """ 
        res = list()
        for virDomain in self.get_vm_alive():
            vm = self.convert_to_entitydomain(virDomain=virDomain)
            if vm != None: res.append(vm)
        return res

    def get_vm_shutdown(self):
        """Retrieve list of VM being shutdown currently as libvirt object
        ----------

        Returns
        -------
        vm_shutdown : list
            list of virDomain
        """
        res = list()
        for domain_name in self.conn.listDefinedDomains():
            try:
                virDomain = self.conn.lookupByName(domain_name)
                res.append(virDomain)
            except libvirt.libvirtError as ex:  # VM is not defined anymore
                pass
        return res

    def get_all_vm(self):
        """Retrieve list of all VM
        ----------

        Returns
        -------
        vm_list : list
            list of virDomain
        """
        vm_list = self.get_vm_alive()
        vm_list.extend(self.get_vm_shutdown())
        return vm_list

    def convert_to_entitydomain(self, virDomain : libvirt.virDomain, force_update = False):
        """Convert the libvirt virDomain object to the domainEntity domain
        ----------

        Parameters
        ----------
        virDomain : libvirt.virDomain
            domain to be converted
        force_update : bool
            Force update of cache

        Returns
        -------
        domain : DomainEntity
            domain as DomainEntity object
        """
        cpu_pin = None
        try:
            # Cache management
            uuid = virDomain.UUIDString()
            if (not force_update) and uuid in self.cache_entity: return self.cache_entity[uuid]
            # General info
            name = virDomain.name()
            mem = virDomain.maxMemory()
            cpu = virDomain.maxVcpus()
            cpu_pin = virDomain.vcpuPinInfo()
            # Custom metadata
            xml_manager = xmlDomainMetaData(xml_as_str=virDomain.XMLDesc())
            xml_manager.convert_to_object()
            if xml_manager.updated() : 
                self.conn.defineXML(xml_manager.convert_to_str_xml()) # Will only be applied after a restart
                print('Warning, no oversubscription found on domain', name, ': defaults were generated')
        except libvirt.libvirtError as ex:  # VM is not alived anymore
            return None
        cpu_ratio = xml_manager.get_oversub_ratios()['cpu']
        # Build entity
        self.cache_entity[uuid] = DomainEntity(uuid=uuid, name=name, mem=mem, cpu=cpu, cpu_pin=cpu_pin, cpu_ratio=cpu_ratio)
        return self.cache_entity[uuid]

    def update_cpu_pinning(self, vm : DomainEntity, virDomain : libvirt.virDomain = None):
        """Update the pinning of a VM to its attribute cpu_pin
        ----------

        Parameters
        ----------
        vm : DomainEntity
            VM model
        virDomain : virDomain
            Libvirt model (retrieve if based on uuid if not specified)
        """
        # Retrieve VM
        vm_pin_current = None
        try:
            if virDomain == None: virDomain = self.conn.lookupByUUIDString(vm.get_uuid())
            vm_pin_current = virDomain.vcpuPinInfo()
            vm_pin_model   = vm.get_cpu_pin()

            for vcpu, cpu_pin_current in enumerate(vm_pin_current):
                if cpu_pin_current != vm_pin_model[vcpu]:
                    virDomain.pinVcpu(vcpu, vm_pin_model[vcpu]) # Live setting
        except libvirt.libvirtError as ex:  # VM is not alived anymore
            pass
        # Update XML desc
        try:
            host_config = len(vm.get_cpu_pin()[0])
            cputune_xml = xmlDomainCputune(xml_as_str=virDomain.XMLDesc(), host_config=host_config, cpupin_per_vcpu=vm.get_cpu_pin())
            virDomain = self.conn.defineXML(cputune_xml.convert_to_str_xml())
        except Exception as ex:
            pass

    def build_cpu_pinning(self, cpu_list : list, host_config : int):
        """Return Libvirt template of cpu pinning based on authorised list of cpu
        ----------

        Parameters
        ----------
        cpu_list : list
            List of ServerCPU 
        host_config : int
           Number of core on host
        Returns
        -------

        template : Tuple
            Pinning template
        """
        template_pin = [False for is_cpu_pinned in range(host_config)]
        for cpu in cpu_list: template_pin[cpu.get_cpu_id()] = True
        return tuple(template_pin)

    def cache_purge(self):
        """Purge cache associating VM uuid to their domainentity representation
        ----------
        """
        del self.cache_entity
        self.cache_entity = dict()

    def get_usage_cpu(self, vm : DomainEntity):
        """Return the latest CPU usage of a given VM. None if unable to compute it (as delta are required)
        ----------

        Parameters
        ----------
        vm : DomainEntity
           VM to consider

        Returns
        -------
        cpu_usage : float
            Usage as [0;1]
        """
        try:
            virDomain = self.conn.lookupByUUIDString(vm.get_uuid())
            epoch_ns = time.time_ns()
            stats = virDomain.getCPUStats(total=True)
        except libvirt.libvirtError as ex:  # VM is not alived
            raise ConsumerNotAlived()
        total, system, user = (stats[0]['cpu_time'], stats[0]['system_time'], stats[0]['user_time'])
        cpu_usage_norm = None
        if vm.has_time(): # Compute delta
            prev_epoch, prev_total, prev_system, prev_user = vm.get_time()
            cpu_usage = (total-prev_total)/(epoch_ns-prev_epoch)
            cpu_usage_norm = cpu_usage / vm.get_cpu()
            if cpu_usage_norm>1: cpu_usage_norm = 1
        vm.set_time(epoch_ns=epoch_ns,total=total, system=system, user=user)
        return cpu_usage_norm

    def get_usage_mem(self, vm : DomainEntity):
        """Return the latest Mem usage of a given VM
        ----------

        Parameters
        ----------
        vm : DomainEntity
           VM to consider

        Returns
        -------
        cpu_usage : float
            Usage as [0;1]
        """
        try:
            virDomain = self.conn.lookupByUUIDString(vm.get_uuid())
            stats = virDomain.memoryStats()
        except libvirt.libvirtError as ex:  # VM is not alived
            raise ConsumerNotAlived()
        #keys = ['actual', 'available', 'rss', 'major_fault']
        usage = stats['rss']/stats['actual']
        if usage>1: return 1
        return usage

    def create_vm(self, vm : DomainEntity):
        """Create a VM based on its DomainEntity description
        ----------

        Parameters
        ----------
        vm : DomainEntity
           VM to consider

        Returns
        -------
        tuple : (bool, reason)
            Success as True/False with reason
        """
        if vm.is_deployed(): raise ValueError('VM already exists')
        vm_xml = self.template_vm.replace('{name}', vm.get_name()).\
                replace('{cpu}', str(vm.get_cpu())).\
                replace('{mem}', str(vm.get_mem(as_kb=True))).\
                replace('{oc_cpu}', str(vm.get_cpu_ratio())).\
                replace('{oc_mem}', str(1.0)).\
                replace('{loc}', self.loc).\
                replace('{machine}', self.machine).\
                replace('{qcow2}', vm.get_qcow2())
        
        # Dynamically add cpupin related data to xml desc
        host_config = len(vm.get_cpu_pin()[0])
        cputune_xml = xmlDomainCputune(xml_as_str=vm_xml, host_config=host_config, cpupin_per_vcpu=vm.get_cpu_pin())
        virDomain = None
        try:
            virDomain = self.conn.defineXML(cputune_xml.convert_to_str_xml())
            virDomain.create()
        except libvirt.libvirtError as ex1:
            try:
                if virDomain != None: virDomain.undefine()
            except libvirt.libvirtError as ex2: 
                pass
            return (False, str(ex1))

        try:
            vm.set_uuid(virDomain.UUIDString())
        except libvirt.libvirtError as ex: 
            return (False, str(ex))
        return (True, None)

    def delete_vm(self, vm : DomainEntity):
        """Delete a VM
        ----------

        Parameters
        ----------
        vm : DomainEntity
           VM to consider

        Returns
        -------
        tuple : (bool, reason)
            Success as True/False with reason
        """
        try:
            virDomain = self.conn.lookupByUUIDString(vm.get_uuid())
        except libvirt.libvirtError as ex: # Already deleted
            return (True, None)
        try:
            virDomain.destroy()
            virDomain.undefine()
        except libvirt.libvirtError as ex:
            return (False, str(ex))
        return (True, None)

    def __del__(self):
        """Clean up actions
        ----------
        """
        try:
            self.conn.close()
        except libvirt.libvirtError as ex:
            pass

class ConsumerNotAlived(Exception):
    pass