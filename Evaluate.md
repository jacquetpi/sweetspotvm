SweetSpotVM principle relies on host oversubscription to exposes cores having different performance capabilities.  
If you wish to evaluate this claim, we propose a minimal experiment composed of two VMs. 

> This test is designed for laptops equipped with a 4 physical cores processor (8 with HT)  
> Do **NOT** use a host with Performance/Energy cores (as these architectures already expose cores with different performances)

## Requirements

You will need two Qcow2 images ready to be used  

## Evaluate performances on a SweetSpotVM setup

Modify your ```.env ``` to apply following template: ```OVSB_TEMPLATE="1.0,1.0,3.0"```
> This template does not oversubscribe the first two vCPUs while proposing others to a 3:1 ratio

Launch the local scheduler
```
(host) source venv/bin/activate
(host) python3 -m schedulerlocal
```

Setup your two VMs (adapt the Qcow2 location)
```
(host) curl 'http://127.0.0.1:8100/deploy?name=vm1&cpu=6&mem=2&qcow2=/var/lib/libvirt/images/vm1.qcow2'
(host) curl 'http://127.0.0.1:8100/deploy?name=vm2&cpu=6&mem=2&qcow2=/var/lib/libvirt/images/vm2.qcow2'
```

Inside VM1, generate some CPU activity
```
(vm1) sudo apt-get install -y stress-ng
(vm1) stress-ng --cpu 0
```

Inside VM2, compare performances between first and last core.
```
(vm2) sudo apt-get install -y stress-ng
(vm2) taskset --cpu 0 stress-ng --cpu 1 -t 60s --metrics-brief
(vm2) taskset --cpu 5 stress-ng --cpu 1 -t 60s --metrics-brief
```
Note the ```bogo ops/s``` value obtained
> We recommend doing 3 to 5 runs

## Expected results

- Under the SweetSpotVM context, the CPU0 should expose better performances than CPU5