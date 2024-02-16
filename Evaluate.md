SweetSpotVM principle relies on host oversubscription to exposes cores having different performance capabilities.  
If you wish to evaluate this claim, we propose a minimal experiment composed of two VMs. 

> This test is designed for laptops equipped with a 4 physical cores processor (8 with hyperthreading)  
> Use the powersave mode of your laptop to get more consistent performances: ```# echo powersave | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor```  
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

Inside VM1, generate some CPU activity (all cores will be used at 50%)
```
(vm1) sudo apt-get install -y stress-ng
(vm1) stress-ng --cpu 0 -l 50
```

Inside VM2, compare performances between the first and the last core using 7zip
```
(vm2) sudo apt-get install -y p7zip-full
(vm2) taskset --cpu 0 7z b 3 -mmt1
(vm2) taskset --cpu 5 7z b 3 -mmt1
```
> Higher performance is shown with an higher ```MIPS``` (million instructions per second)

Note the average (Avr)  ```MIPS``` values obtained from both Compressing and Decompressing steps

## Expected results

- Under the SweetSpotVM context, the CPU0 should expose better performances than the CPU5 (we observed a delta of 5-10% on a i7-1185G7 platform)