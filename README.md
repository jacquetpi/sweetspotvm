SweetSpotVM is a prototype showing how vCPUs of a given VM can be oversubscribed to different levels.
It is composed of a local scheduler, deployed on each server, which manage deployments requests and adapt the pinning accordingly.

The server must be equipped with QEMU/KVM and libvirt for online execution

## Setup

```bash
apt-get update && apt-get install -y git python3 python3.venv
git clone https://github.com/jacquetpi/sweetspotvm
cd sweetspotvm/
cp dotenv .env
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```

Configuration is being made by the ```.env``` file  
Oversubscription template can be changed using this file

## Local scheduler - Offline mode

As an experience may be fastidious to set up, our local scheduler as an offline mode for quick execution  
By loading an host CPU topology (*the platform*) and a workload (*the input*), anyone can execute a previously executed experiment.  
The workload trace is an heavy file. We provide an example [hosted separately](https://drive.google.com/uc?id=1sZUOsvNZ4yB1ckW5o3AR5Fg-jZWBEk-W&export=download) that should be placed in ```debug``` folder

- Offline execution 
```bash
source venv/bin/activate
tar -xzvf debug/monitoring-EPYC7662-vertical-oc2.tar.gz -C debug/
python3 -m schedulerlocal --topology=debug/topology_EPYC-7662-exp.json --load=debug/monitoring-EPYC7662-vertical-oc2.csv --debug=1
```
> Load an EPYC-7662 platform jointly with a corresponding workload  
> The debug=1 generates a new ```debug/monitoring.csv``` trace based on re-computation. 
> This is a time consuming step, as the physical equivalent experiment was performed on a 12 hours window, therefore generating lot of data

After that, executing cells sequentially in notebook ```demo.ipynb```  allows to re-generate figure 5 of the paper using this trace

## Local scheduler - Online mode

Instance on each server  
Deployed by default on 8100 port. Can be changed through the ```.env``` file
```bash
python3 -m schedulerlocal --help
```

- Online execution
```bash
source venv/bin/activate
python3 -m schedulerlocal
```

The local scheduler will run and wait for requests  
Requests are made in a REST fashion way

- On a live setting : Order the creation of a vm
```bash
curl 'http://127.0.0.1:8100/deploy?name=example&cpu=2&mem=2&qcow2=/var/lib/libvirt/images/hello.qcow2'
```
> name: VM name (must be unique)  
> cpu: vCPU requested  
> mem: GB requested (may be a float)  
> qcow2: QCOW2 image (must be pre-existant, typically on a distributed storage mount point)

- Online execution : Order the deletion of a vm
```bash
curl 'http://127.0.0.1:8100/remove?name=example'
```
