import threading, os, json
from flask import Flask
from flask import request
from waitress import serve
from schedulerlocal.domain.domainentity import DomainEntity

class ApiEndpoint(object):
    """
    The API endpoint exposes VM management using REST protocol
    ...

    Attributes
    ----------
    subset_manager_pool : SubsetManagerPool
        Subset Manager pool

    Public Methods
    -------
    todo()
        todo
    """
    
    def __init__(self, **kwargs):
        req_attributes = ['api_url', 'api_port', 'scheduler_global']
        for req_attribute in req_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', req_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])

    def run(self):
        """Run REST API on a separate thread
        ----------
        """
        def target():
            self.app = self.create_app()
            print('Exposing schedulerglobal API on http://' + self.api_url + ':' + str(self.api_port))
            serve(self.app, host=self.api_url, port=self.api_port,  threads=1)

        self.thread = threading.Thread(target=target)
        self.thread.start()

    def create_app(self):
        """Create Flask REST app with appropriate routes callback
        ----------
        """
        app = Flask('myapp')

        app.route('/', endpoint='home', methods = ['GET'])(lambda: self.home())
        app.route('/status', endpoint='status', methods = ['GET'])(lambda: self.status())
        app.route('/deploy', endpoint='deploy', methods = ['GET'])(lambda: self.deploy())
        app.route('/remove', endpoint='remove', methods = ['GET'])(lambda: self.remove())

        return app
    
    def home(self):
        """Root uri
        ----------
        """
        return 'SchedulerGlobal is working and waiting for instructions'

    def status(self):
        """/info uri : displaying status 
        ----------
        """
        return self.scheduler_global.status()

    def deploy(self):
        """/deploy uri : deploying a new VM
        ----------
        """
        usage = 'Wrong usage: http://' + self.api_url + ':' + str(self.api_port) + '/deploy?name=example&cpu=1&mem=1&oc=1&qcow2=/var/lib/libvirt/images/volume.qcow2'

        args_required = ['name', 'cpu', 'mem', 'oc', 'qcow2']
        for arg in args_required:
            if request.args.get(arg) is None: return usage

        return self.scheduler_global.deploy(name=request.args.get('name'),\
            cpu=request.args.get('cpu'), memory=request.args.get('mem'),\
            ratio=request.args.get('oc'), disk=request.args.get('qcow2'))

    def remove(self):
        """/remove uri : Remove a VM identified by its name
        ----------
        """
        usage = 'Wrong usage: http://' + self.api_url + ':' + str(self.api_port) + '/remove?name=example'

        args_required = ['name']
        for arg in args_required:
            if request.args.get(arg) is None: return usage
            
        return self.scheduler_global.remove(name=request.args.get('name'))

    def shutdown(self):
        """Manage thread shutdown
        ----------
        """
        raise os._exit(0)