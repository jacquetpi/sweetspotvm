from xml.dom import minidom

class xmlObject(object):
    """
    Advanced Libvirt configuration relies sometimes on XML modification.
    xmlObject and its child classes allows to modify specific portion of a XML libvirt domain description
    /!\ All XML modification must kept in mind that they will only take effect after a full reboot of targeted VM /!\
    ...

    Attributes
    ----------
    xml : minidom.Document
        XML document

    Public Methods
    -------
    convert_to_object()
        Using xml attribute, convert xml "data of interest" to attributes. Child specific. Must be reimplemented
    convert_to_str_xml()
        Return an XML string of current state
    update_dom()
        Using current object attributes, update xml document. Child specific. Must be reimplemented
    get_all_dom()
        Return root XML minidom.Document jointly with specific dom (refer to get_dom_specific)
    get_dom_root()
        Return root XML minidom.Document
    get_dom_specific()
        Return element of interest in XML. Each child class typically target access/modification of a specific XML element. Child specific. Must be reimplemented
    parse()
        Convert an XML string to a XML minidom.Document
    """

    def __init__(self, xml_as_document = None, xml_as_str = None):
        self.xml = None
        if   xml_as_document is not None: self.xml = xml_as_document
        elif xml_as_str     is not None: self.xml = self.parse(xml_as_str)
        if self.xml is not None: self.convert_to_object()

    def convert_to_object(self):
        """Using xml attribute, convert xml "data of interest" to attributes. Child specific. Must be reimplemented
        ----------
        """
        raise NotImplementedError()

    def convert_to_str_xml(self):
        """Return an XML string of current state
        ----------
        """
        dom_root, dom_targeted = self.get_all_dom()
        self.update_dom(dom_targeted)
        return dom_root.toxml()

    def update_dom(self, dom_targeted : minidom.Document):
        """Using current object attributes, update xml document. Child specific. Must be reimplemented
        ----------

        Parameters
        ----------
        dom_targeted : minidom.Document
            the document to consider while updating attributes
        """
        raise NotImplementedError()

    def get_all_dom(self):
        """Return root XML minidom.Document jointly with specific dom (refer to get_dom_specific)
        ----------
        """
        return self.get_dom_root(), self.get_dom_specific(self.get_dom_root())

    def get_dom_root(self):
        """Return root XML minidom.Document
        ----------
        """
        return self.xml

    def get_dom_specific(self, dom_root : minidom.Document):
        """Return element of interest in XML. Each child class typically target access/modification of a specific XML element. 
        Child must reiplement it and may call this super implemtation to refer to default cpu element
        ----------
        """
        dom_cpu_list =  dom_root.getElementsByTagName("cpu")
        if len(dom_cpu_list) != 1: raise ValueError("Incorrect number of cpu node in xml", len(dom_cpu_list))
        return dom_cpu_list[0]

    def parse(self, to_be_parsed : str):
        """Convert an XML string to a XML minidom.Document
        ----------
        """
        return minidom.parseString(to_be_parsed)

class xmlDomainCpuNumaCell(xmlObject):
    """
    Allow modification of a NumaCell XML element. Must be used jointly with xmlDomainNuma as numacell is a child element.
    ...

    Public Methods reimplemented/introduced
    -------
    convert_to_object()
        Using xml attribute, convert xml data related to numa cell to object attributes.
    update_dom()
        Using current object numa attributes, update xml document
    get_dom_specific()
        Return Numa cell XML element
    """

    def __init__(self, xml_as_document = None, xml_as_str = None, id : int = None, cpu_count : int = None,):
        self.id=id
        self.cpu_count=cpu_count # to generate default
        self._cell_attributes = ['id', 'cpus', 'memory', 'unit']
        self.cells = dict()
        self.distances = dict()
        super().__init__(xml_as_document=xml_as_document,xml_as_str=xml_as_str)

    def __initialize_default_cell(self, dom_root : minidom.Document):
        """Initialize a default NUMA cell in XML
        
        Parameters
        ----------
        dom_root : minidom.Document
            the document to consider while creating element
        """
        # Initialize object attribute
        self.cells = dict()
        self.cells['id'] = str(self.id)
        self.cells['cpus'] = str(self.id)
        self.cells['memory'] = '512000'
        self.cells['unit'] = 'KiB'
        for index in range(self.cpu_count):
            if index == self.id:
                self.distances[index]=10
            else:
                self.distances[index]=20

        # Initialize xml cell
        dom_cell =  dom_root.createElement('cell')
        for attribute in self._cell_attributes: dom_cell.setAttribute(attribute, self.cells[attribute])
        # Initialize xml cell siblings values
        dom_distances =  dom_root.createElement('distances')
        dom_cell.appendChild(dom_distances)
        for distance_id, distance_val in self.distances.items():
            dom_sibling =  dom_root.createElement('sibling')
            dom_sibling.setAttribute('id', str(distance_id))
            dom_sibling.setAttribute('value', str(distance_val))
            dom_distances.appendChild(dom_sibling)
        return dom_cell

    def convert_to_object(self):
        """Using xml attribute, convert xml data related to numa cell to object attributes.
        ----------
        """
        dom_cell = self.get_dom_specific(self.get_dom_root())
        for attribute in self._cell_attributes:
            self.cells[attribute] = dom_cell.getAttribute(attribute)

        dom_distances_list = dom_cell.getElementsByTagName('distances')
        if len(dom_distances_list)<1:
            print("Warning, no distances found")
        elif len(dom_distances_list)==1:
            dom_distances = dom_distances_list[0]
            dom_sibling_list = dom_distances.getElementsByTagName('sibling')
            for dom_sibling in dom_sibling_list:
                self.distances[dom_sibling.getAttribute('id')] = dom_sibling.getAttribute('value')
        else:
            raise ValueError("Incorrect number of 'distances' node in xml", len(dom_distances_list))

    def update_dom(self, dom_targeted : minidom.Element):
        """Using current object numa cell attributes, update xml document
        
        Parameters
        ----------
        dom_targeted : minidom.Document
            the document to consider while updating attributes
        """
        for attribute in self._cell_attributes : dom_targeted.setAttribute(attribute, self.cells[attribute])

    def get_dom_specific(self, dom_root : minidom.Document):
        """Return Numa cell XML element (create it if not present)
        
        Parameters
        ----------
        dom_root : minidom.Document
            the document to consider while searching element
        """
        dom_cpu = super().get_dom_specific(dom_root)

        dom_numa_list = dom_cpu.getElementsByTagName('numa')
        dom_numa = None
        if len(dom_numa_list) == 0:
            dom_numa =  dom_root.createElement('numa')
            dom_cpu.appendChild(dom_numa)
        else:
            dom_numa =  dom_numa_list[0]

        if dom_numa == None: raise ValueError("Incorrect number of 'topology' node in xml", len(dom_numa_list))
        return self.__get_dom_cell_in_numa(dom_root, dom_numa)

    def __get_dom_cell_in_numa(self, dom_root : minidom.Document, dom_numa : minidom.Element):
        """Return Numa cell XML element (create it if not present)
        
        Parameters
        ----------
        dom_root : minidom.Document
            the document to consider if creation is needed
        dom_numa : minidom.Element
            the element to start searching from
        """
        dom_cell_list = dom_numa.getElementsByTagName('cell')

        dom_cell = None
        for dom_cell_tested in dom_cell_list:
            if dom_cell_tested.getAttribute('id') == str(self.id):
                dom_cell = dom_cell_tested

        if dom_cell == None:
            dom_cell = self.__initialize_default_cell(dom_root)
            dom_numa.appendChild(dom_cell)

        return dom_cell

    def __str__(self):
        """
        Return a string representation of the cell
        ----------
        """
        return 'cell id=' + str(self.cells['id']) +' cpus=' + str(self.cells['cpus']) + ' memory=' + str(self.cells['memory']) +' unit=' + str(self.cells['unit']) + ' distances: ' + str(self.distances) + '\n'

class xmlDomainNuma(xmlObject):
    """
    Allow modification of a CPU numa XML element.
    ...

    Public Methods reimplemented/introduced
    -------
    convert_to_object()
        Using xml attribute, convert xml data related to numa node to object attributes.
    update_dom()
        Using current object numa attributes, update xml document
    get_dom_specific()
        Return Numa node XML element
    get_topology_as_dict()
        Return CPU topology found as dict
    set_topology_as_dict()
        Set custom topology
    get_cpu_count()
        Compute and return cpu count from NUMA specification
    """

    def __init__(self, xml_as_document = None, xml_as_str = None):
        self._topology_attributes = ['sockets', 'dies', 'cores', 'threads']
        self.topology = dict()
        for attribute in self._topology_attributes : self.topology[attribute] = None
        self.numa_cells = list()
        super().__init__(xml_as_document=xml_as_document,xml_as_str=xml_as_str)

    def convert_to_object(self):
        """Using xml attribute, convert xml data related to numa node to object attributes.
        ----------
        """
        dom_root, dom_topology = self.get_all_dom()
        for attribute in self._topology_attributes : self.topology[attribute] = dom_topology.getAttribute(attribute)

        dom_numa_list = dom_topology.getElementsByTagName('numa')
        if len(dom_numa_list)<1:
            print("Warning, no NUMA configuration found, generating default")
            count = self.get_cpu_count()
            for id in range(count): self.numa_cells.append(xmlDomainCpuNumaCell(xml_as_document=dom_root, id=id, cpu_count=count))
        elif len(dom_numa_list)==1:
            dom_numa = dom_numa_list[0]
            dom_numa_cell_list = dom_numa.getElementsByTagName('cell')
            for dom_numa_cell in dom_numa_cell_list: self.numa_cells.append(xmlDomainCpuNumaCell(xml_as_document=dom_root))
        else:
            raise ValueError("Incorrect number of 'numa' node in xml", len(dom_numa_list))

    def get_topology_as_dict(self):
        """Return CPU topology found as dict
        ----------
        """
        return self.topology

    def set_topology_as_dict(self, topology):
        """Set custom topology
        ----------
        """
        self.topology=topology

    def get_cpu_count(self):
        """Compute and return cpu count from NUMA specification
        ----------
        """
        count=1
        for attribute in self._topology_attributes:
            if attribute != 'dies': count*= int(self.topology[attribute])
        return count

    def update_dom(self, dom_targeted : minidom.Element):
        """Using current object numa attributes, update xml document
        
        Parameters
        ----------
        dom_targeted : minidom.Document
            the document to consider while updating attributes
        """
        if dom_targeted==None: dom_targeted=self.get_dom_specific()
        # Update current object
        for attribute in self._topology_attributes : dom_targeted.setAttribute(attribute, self.topology[attribute])
        # Update child objects
        for numa_cell in self.numa_cells: numa_cell.update_dom(dom_targeted=numa_cell.get_dom_specific(numa_cell.get_dom_root()))
        return dom_targeted

    def get_dom_specific(self, dom_root : minidom.Document):
        """Return topology XML element
        
        Parameters
        ----------
        dom_root : minidom.Document
            the document to consider while searching element
        """
        dom_cpu = super().get_dom_specific(dom_root)

        dom_topology_list = dom_cpu.getElementsByTagName('topology')
        if len(dom_topology_list) != 1: raise ValueError("Incorrect number of 'topology' node in xml", len(dom_topology_list))
        return dom_topology_list[0]

    def __str__(self):
        """
        Return a string representation of CPU topology
        ----------
        """
        return ' '.join([attribute + ':' + str(self.topology[attribute]) for attribute in self._topology_attributes]) + '\n' +\
             ''.join(['  ' + str(numa_cell) for numa_cell in self.numa_cells])

class xmlDomainMetaData(xmlObject):
    """
    Allow modification of oversubscription related metadata (custom field from our implementation)
    ...

    Public Methods reimplemented/introduced
    -------
    convert_to_object()
        Using xml attribute, convert xml data related to numa node to object attributes.
    update_dom()
        Using current object numa attributes, update xml document
    get_dom_specific()
        Return Numa node XML element
    updated()
        Return true if XML modification occured (due to missing fields). False otherwise.
    get_oversub_ratios()
        Return dict of oversubscription found
    """

    def __init__(self, xml_as_document = None, xml_as_str = None):
        self.oversub_attributes = ['cpu', 'mem', 'disk', 'network']
        self.oversub = dict()
        self.was_updated = False
        super().__init__(xml_as_document=xml_as_document,xml_as_str=xml_as_str)

    def get_dom_specific(self, dom_root : minidom.Document):
        """Return oversubscription metadata cell element (create it if not present)
        
        Parameters
        ----------
        dom_root : minidom.Document
            the document to consider while searching element
        """
        # Focus on metadata node
        dom_metadata_list =  dom_root.getElementsByTagName("metadata")
        if len(dom_metadata_list) > 1: 
            raise ValueError("Incorrect number of metadata node in xml", len(dom_metadata_list))
        elif len(dom_metadata_list) == 0:
            dom_metadata =  dom_root.createElement('metadata')
            dom_root.appendChild(dom_metadata)
        else: dom_metadata = dom_metadata_list[0]

        # Focus on oversubscription node
        dom_oversub_list = dom_metadata.getElementsByTagName('sched:ratio')
        if len(dom_oversub_list)>1:
            raise ValueError("Incorrect number of 'sched:ratio' node in xml", len(dom_oversub_list))
        elif len(dom_oversub_list)<1:
            defaults = {'xmlns:sched':'1.0.0', 'cpu':1.0, 'mem':1.0, 'disk':1.0, 'network':1.0}
            dom_oversub = dom_root.createElement('sched:ratio')
            dom_metadata.appendChild(dom_oversub)
            for default_key, default_value in defaults.items(): dom_oversub.setAttribute(default_key, default_value)
            self.was_updated = True
        else: dom_oversub = dom_oversub_list[0]
        
        return dom_oversub
            
    def convert_to_object(self):
        """Using xml attribute, convert xml data related oversubscription to object attributes.
        ----------
        """
        dom_cell = self.get_dom_specific(self.get_dom_root())
        for attribute in self.oversub_attributes:
            self.oversub[attribute] = float(dom_cell.getAttribute(attribute))

    def update_dom(self, dom_targeted : minidom.Element):
        """Using current object metadata attributes, update xml document
        
        Parameters
        ----------
        dom_targeted : minidom.Document
            the document to consider while updating attributes
        """
        for attribute in self.oversub_attributes: dom_targeted.setAttribute(attribute, str(self.oversub[attribute]))

    def updated(self):
        """Return true if XML modification occured (due to missing fields). False otherwise.
        ----------
        """
        return self.was_updated

    def get_oversub_ratios(self):
        """Return dict of oversubscription found
        ----------
        """
        return self.oversub

class xmlDomainCputunePin(xmlObject):
    """
    Allow modification of a vcpupin XML element. Must be used jointly with xmlDomainCputune as vcpupin is a child element.
    ...

    Public Methods reimplemented/introduced
    -------
    convert_to_object()
        Using xml attribute, convert xml data related to vcpupin cell to object attributes.
    update_dom()
        Using current object vcpupin attributes, update xml document
    get_dom_specific()
        Return vcpupin cell XML element
    """

    def __init__(self, dom_cputune : minidom.Element, host_config : int, xml_as_document = None, xml_as_str = None, vcpu : int = None, cpu_template : tuple = None):
        self.dom_cputune = dom_cputune
        self.host_config = host_config # number of cpu on host
        self.vcpu = vcpu
        self.cpu_template = cpu_template
        super().__init__(xml_as_document=xml_as_document,xml_as_str=xml_as_str)

    def get_dom_specific(self, dom_root : minidom.Document):
        """Return Numa cell XML element (create it if not present)
        
        Parameters
        ----------
        dom_root : minidom.Document
            the document to consider while searching element
        """
        dom_vcpupin_list = self.dom_cputune.getElementsByTagName('vcpupin')
        dom_vcpupin = None
        for dom_vcpupin_tested in dom_vcpupin_list:
            if dom_vcpupin_tested.getAttribute('vcpu') == str(self.vcpu):
                dom_vcpupin = dom_vcpupin_tested
                break

        if dom_vcpupin == None:
            dom_vcpupin =  dom_root.createElement('vcpupin')
            self.update_dom(dom_vcpupin)
            self.dom_cputune.appendChild(dom_vcpupin)

        return dom_vcpupin

    def convert_to_object(self):
        """Using xml attribute, convert xml data related to numa cell to object attributes.
        ----------
        """
        dom_vcpupin = self.get_dom_specific(self.get_dom_root())

        self.vcpu = dom_vcpupin.getAttribute('vcpu')
        self.cpu_template = self.__get_cpu_template_from_regex(dom_vcpupin.getAttribute('cpuset'))

    def update_dom(self, dom_targeted : minidom.Element):
        """Using current object attributes, update xml document
        
        Parameters
        ----------
        dom_targeted : minidom.Document
            the document to consider while updating attributes
        """
        dom_targeted.setAttribute('vcpu', str(self.vcpu))
        dom_targeted.setAttribute('cpuset', self.__get_cpuset_as_regex())

    def __get_cpuset_as_regex(self):
        """Using cpu template attribute, return the equivalent regex expression
        
        Return
        ----------
        regex : str
            CPU authorized in regex form
        """
        cpuset_str = ''.join([',' + str(cpuid) if is_pinned else '' for cpuid, is_pinned in enumerate(self.cpu_template)])
        return cpuset_str.replace(',', '', 1) # remove first coma

    def __get_cpu_template_from_regex(self, regex_cpu :str):
        """Using regex str, return the equivalent cpu template
        
        Return
        ----------
        template : tuple
            CPU authorized in tuple form
        """
        self.cpu_template = [False for i in range(self.host_config)]
        for cpuid in regex_cpu.split(','): self.cpu_template[int(cpuid)] = True
        return tuple(self.cpu_template) # convert list to tuple

    def get_vcpu(self):
        """Getter on vcpu attribute
        ----------
        """
        return self.vcpu

    def get_cpu_template(self):
        """Getter on cpu template
        ----------
        """
        return self.cpu_template

class xmlDomainCputune(xmlObject):
    """
    Allow modification of cputune element
    ...

    Public Methods reimplemented/introduced
    -------
    convert_to_object()
        Using xml attribute, convert xml data related to cputune node to object attributes.
    update_dom()
        Using current authorized cpuid, update document
    get_dom_specific()
        Return cputune XML element
    """

    def __init__(self, host_config : int, xml_as_document = None, xml_as_str = None, cpupin_per_vcpu : list = None):
        self.host_config = host_config
        self.cpupin_per_vcpu = cpupin_per_vcpu
        self.vcpupin_list = list()
        super().__init__(xml_as_document=xml_as_document,xml_as_str=xml_as_str)

    def get_dom_specific(self, dom_root : minidom.Document):
        """Return cputune element
        
        Parameters
        ----------
        dom_root : minidom.Document
            the document to consider while searching element
        """
        # Focus on metadata node
        dom_cputune_list =  dom_root.getElementsByTagName('cputune')
        if len(dom_cputune_list) > 1: 
            raise ValueError('Incorrect number of cputune node in xml', len(dom_cputune_list))
        elif len(dom_cputune_list) == 0:
            # Retrieve domain
            dom_domain_list =  dom_root.getElementsByTagName('domain')
            if len(dom_domain_list) !=1: raise ValueError('Incorrect number of domain node in xml', len(dom_domain_list))
            dom_cputune =  dom_root.createElement('cputune')
            dom_domain_list[0].appendChild(dom_cputune)
            for vcpu, cpu_template in enumerate(self.cpupin_per_vcpu): 
                self.vcpupin_list.append(xmlDomainCputunePin(xml_as_document=dom_root, dom_cputune=dom_cputune, host_config=self.host_config, vcpu=vcpu, cpu_template=cpu_template))
        else: dom_cputune = dom_cputune_list[0]
        return dom_cputune

    def convert_to_object(self):
        """Using xml attribute, convert xml data related to cputune node to object attributes.
        ----------
        """
        self.vcpupin_list = list()
        for vcpupin in self.vcpupin_list: 
            vcpupin.append(self.vcpupin.get_cpu_template())

    def update_dom(self, dom_targeted : minidom.Element):
        """Using current object cpututne attributes, update xml document
        
        Parameters
        ----------
        dom_targeted : minidom.Document
            the document to consider while updating attributes
        """
        if dom_targeted==None: dom_targeted=self.get_dom_specific()
        # Update child objects
        for vcpupin in self.vcpupin_list: 
            vcpupin.update_dom(dom_targeted=vcpupin.get_dom_specific(vcpupin.get_dom_root()))
        return dom_targeted

    def get_cpupin_per_vcpu(self):
        """Getter on cpupin_per_vcpu attribute
        ----------
        """
        return self.cpupin_per_vcpu