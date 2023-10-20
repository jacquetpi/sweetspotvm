Intro

## Setup

```bash
apt-get update && apt-get install -y git python3 python3.venv
git clone https://github.com/jacquetpi/vmpinning
cd vmpinning/
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Local

```bash
python3 -m schedulerlocal -h
python3 -m schedulerlocal --load=debug/topology_EPYC-7662.json
python3 -m schedulerlocal --load=debug/topology_i7-1185G7.json
python3 -m schedulerlocal --debug=1
```

Order the creation of a vm
```bash
curl 'http://127.0.0.1:8099/deploy?name=example&cpu=2&mem=2&oc=2&qcow2=/var/lib/libvirt/images/hello.qcow2'
```

Order the deletion of a vm
```bash
curl 'http://127.0.0.1:8099/remove?name=example'
```

Offline setting
```bash
python3 -m schedulerlocal --load=results/monitoring.csv --topology=debug/topology_EPYC-7662-exp.json
```

## Global

TODO