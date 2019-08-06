# DER-Dispatch-app

## Purpose

The purpose of this repository is to document the chosen way of registering and running applications within a 
GridAPPS-D deployment.

## Requirements

1. Docker ce version 17.12 or better.  You can install this via the docker_install_ubuntu.sh script.  (note for mint you will need to modify the file to work with xenial rather than ubuntu generically)

## Quick Start

1. Please clone the repository <https://github.com/GRIDAPPSD/gridappsd-docker> (refered to as gridappsd-docker repository) next to this repository (they should both have the same parent folder)

    ```console
    git clone https://github.com/GRIDAPPSD/gridappsd-docker
    git clone https://github.com/GRIDAPPSD/DER-Dispatch-app
    
    ls -l
    
    drwxrwxr-x  7 osboxes osboxes 4096 Sep  4 14:56 gridappsd-docker
    drwxrwxr-x  5 osboxes osboxes 4096 Sep  4 19:06 DER-Dispatch-app

    ```

## Creating the sample-app application container

1.  From the command line execute the following commands to build the sample-app container

    ```console
    osboxes@osboxes> cd DER-Dispatch-app
    osboxes@osboxes> docker build --network=host -t der-dispatch-app .
    ```

1.  Add the following to the gridappsd-docker/docker-compose.yml file

    ```yaml
      derdispatch:
        image: der-dispatch-app
        ports:
          - 9001:9001
        environment:
          GRIDAPPSD_URI: tcp://gridappsd:61613
        depends_on:
          - gridappsd
    ```

1.  Run the docker application 

    ```console
    osboxes@osboxes> cd gridappsd-docker
    osboxes@osboxes> ./run.sh
    
    # you will now be inside the container, the following starts gridappsd
    
    gridappsd@f4ede7dacb7d:/gridappsd$ ./run-gridappsd.sh
    
    ```

4. Application Configuration
Set start time to 2013-07-22 10:32:00

    ```json
    {'OPF': 1,
     'run_freq': 60,
     'run_on_host': true,
     'run_realtime': true,
     'stepsize_xp': 0.2,
     'stepsize_xq': 2,
     'coeff_p': 0.1,
     'coeff_q': 0.00005,
     'stepsize_mu': 500}
    ```

Next to start the application through the viz follow the directions here: https://gridappsd.readthedocs.io/en/latest/using_gridappsd/index.html#start-gridapps-d-platform

```bash
python /usr/src/gridappsd-der-dispatch/der_dispatch_app/main_app_new.py 952325492 '{"power_system_config":{"SubGeographicalRegion_name":"_1CD7D2EE-3C91-3248-5662-A43EFEFAC224","GeographicalRegion_name":"_24809814-4EC6-29D2-B509-7F8BFB646437","Line_name":"_EBDB5A4A-543C-9025-243E-8CAD24307380"},"simulation_config":{"power_flow_solver_method":"NR","duration":600,"simulation_name":"ieee123","simulator":"GridLAB-D","start_time":1374510720,"run_realtime":false,"simulation_output":{},"model_creation_config":{"load_scaling_factor":1.0,"triplex":"y","encoding":"u","system_frequency":60,"voltage_multiplier":1.0,"power_unit_conversion":1.0,"unique_names":"y","schedule_name":"ieeezipload","z_fraction":0.0,"i_fraction":1.0,"p_fraction":0.0,"randomize_zipload_fractions":false,"use_houses":false},"simulation_broker_port":59469,"simulation_broker_location":"127.0.0.1"},"application_config":{"applications":[{"name":"der_dispatch_app","config_string":"{\"OPF\": 0, \"run_freq\": 60, \"run_on_host\": false, \"run_realtime\": false, \"stepsize_xp\": 0.2, \"stepsize_xq\": 2, \"coeff_p\": 0.1, \"coeff_q\": 5e-05, \"stepsize_mu\": 500}"}]},"simulation_request_type":"NEW"}' '{OPF:0,run_freq:60,run_on_host:false,run_realtime:false,stepsize_xp:0.2,stepsize_xq:2,coeff_p:0.1,coeff_q:5e-05,stepsize_mu:500}'
```