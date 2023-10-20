import requests

class ApiRequester(object):
    """
    The API requester manage calls to localschedulers instance
    ...

    Attributes
    ----------
    url : str
        Url of the global scheduller endpoint

    Public Methods
    -------
    deploy_vm()
        Deploy a VM to the cluster
    remove_vm()
        Remove a VM to the cluster
    info()
        Display current cluster state
    """
    
    def __init__(self, **kwargs):
        req_attributes = ['url']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])

    def deploy(self, name : str, cpu : str, memory : str, ratio : str, disk : str):
        """Ask the Global scheduler to deploy a VM based on requested specificatio
        ----------

        Parameters
        ----------
        name : str
            VM name as str
        cpu : str
            Number of vcpu as str
        memory : str
            Memory (gb) as str
        ratio :  str
            Premium policy to apply
        disk :  str
            Disk location

        Returns
        -------
        response : str
            Return result of operation as str
        """
        constructed_url = self.url + '/deploy?name=' + str(name) + '&cpu=' + str(cpu) + '&mem=' + str(memory) +\
             '&oc=' + str(ratio) + '&qcow2=' + str(disk)
        response = requests.get(constructed_url)
        print(constructed_url)
        return response.json()

    def remove(self, name : str):
        """Ask the Global scheduler to remove a specific VM
        ----------

        Parameters
        ----------
        vm : str
            Name of VM to be removed

        Returns
        -------
        response : str
            Return result of operation as str
        """
        constructed_url = self.url + '/remove?name=' + str(name)
        response = requests.get(constructed_url)
        return response.json()

    def status(self):
        """Return the current cluster state
        ----------

        Returns
        -------
        state : str
            Cluster related info as str
        """
        constructed_url = self.url + '/status'
        response = requests.get(constructed_url)
        return response.json()