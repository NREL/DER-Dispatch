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

## Creating the der-dispatch-app application container

1.  From the command line execute the following commands to build the der-dispatch-app container

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
        volumes:
          - $HOME/git/DER-Dispatch-app-Public:/usr/src/gridappsd-der-dispatch
    ```

1.  Run the docker application 

    ```console
    osboxes@osboxes> cd gridappsd-docker
    osboxes@osboxes> ./run.sh
    
    # you will now be inside the container, the following starts gridappsd
    
    gridappsd@f4ede7dacb7d:/gridappsd$ ./run-gridappsd.sh
    
    ```

1. Application Configuration

Set start time to 2013-07-22 10:30:00

Options:
Run with OPF = 0  and a specific time and duration first to get the baseline for comparison.
Then Run with OPF = 1 and the same time and duration evaluate the applications performance against the baseline.
run_freq - Number of seconds between appliction runs.
run_on_host - True then run on the host machine. False then run in the container.



The baseline will be in this folder in the container:
/usr/src/gridappsd-der-dispatch/der_dispatch_app/adms_result_ieee123pv_house_1563893400_OPF_0_

### Baseline run

```json
    {
      "OPF": 0,
      "run_freq": 15,
      "run_on_host": false,
      "run_realtime": true,
      "stepsize_xp": 0.2,
      "stepsize_xq": 2,
      "coeff_p": 0.005,
      "coeff_q": 0.005,
      "Vupper": 1.035,
      "Vlower": 0.96,
      "stepsize_mu": 50000,
      "optimizer_num_iterations": 10
    }

```

### Optimal Powerflow on Run

Set OPF to 1 and run again.

```json
    {
      "OPF": 1,
      "run_freq": 15,
      "run_on_host": false,
      "run_realtime": true,
      "stepsize_xp": 0.2,
      "stepsize_xq": 2,
      "coeff_p": 0.005,
      "coeff_q": 0.005,
      "Vupper": 1.035,
      "Vlower": 0.96,
      "stepsize_mu": 50000,
      "optimizer_num_iterations": 10
    }
```
    
1. docker copy command
```bash
docker cp 422635e932bb:/usr/src/gridappsd-der-dispatch/der_dispatch_app/adms_result_test9500new_1374510720_OPF_1_stepsize_xp_0_2_stepsize_xq_2_coeff_p_0_1_coeff_q_5e_neg_05_stepsize_mu_500/ $HOME/
```

Next to start the application through the viz follow the directions here: https://gridappsd.readthedocs.io/en/latest/using_gridappsd/index.html#start-gridapps-d-platform





```bash
python /usr/src/gridappsd-der-dispatch/der_dispatch_app/main_app_new.py 952325492 '{"power_system_config":{"SubGeographicalRegion_name":"_1CD7D2EE-3C91-3248-5662-A43EFEFAC224","GeographicalRegion_name":"_24809814-4EC6-29D2-B509-7F8BFB646437","Line_name":"_EBDB5A4A-543C-9025-243E-8CAD24307380"},"simulation_config":{"power_flow_solver_method":"NR","duration":600,"simulation_name":"ieee123","simulator":"GridLAB-D","start_time":1374510720,"run_realtime":false,"simulation_output":{},"model_creation_config":{"load_scaling_factor":1.0,"triplex":"y","encoding":"u","system_frequency":60,"voltage_multiplier":1.0,"power_unit_conversion":1.0,"unique_names":"y","schedule_name":"ieeezipload","z_fraction":0.0,"i_fraction":1.0,"p_fraction":0.0,"randomize_zipload_fractions":false,"use_houses":false},"simulation_broker_port":59469,"simulation_broker_location":"127.0.0.1"},"application_config":{"applications":[{"name":"der_dispatch_app","config_string":"{\"OPF\": 0, \"run_freq\": 60, \"run_on_host\": false, \"run_realtime\": false, \"stepsize_xp\": 0.2, \"stepsize_xq\": 2, \"coeff_p\": 0.1, \"coeff_q\": 5e-05, \"stepsize_mu\": 500}"}]},"simulation_request_type":"NEW"}' '{OPF:0,run_freq:60,run_on_host:false,run_realtime:false,stepsize_xp:0.2,stepsize_xq:2,coeff_p:0.1,coeff_q:5e-05,stepsize_mu:500}'
```

```bash
python /usr/src/gridappsd-der-dispatch/der_dispatch_app/main_app_new.py 1522305637 '{"power_system_config":{"SubGeographicalRegion_name":"_1CD7D2EE-3C91-3248-5662-A43EFEFAC224","GeographicalRegion_name":"_24809814-4EC6-29D2-B509-7F8BFB646437","Line_name":"_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D"},"simulation_config":{"power_flow_solver_method":"NR","duration":600,"simulation_name":"ieee123","simulator":"GridLAB-D","start_time":1374510720,"run_realtime":false,"simulation_output":{},"model_creation_config":{"load_scaling_factor":1.0,"triplex":"y","encoding":"u","system_frequency":60,"voltage_multiplier":1.0,"power_unit_conversion":1.0,"unique_names":"y","schedule_name":"ieeezipload","z_fraction":0.0,"i_fraction":1.0,"p_fraction":0.0,"randomize_zipload_fractions":false,"use_houses":false},"simulation_broker_port":59469,"simulation_broker_location":"127.0.0.1"},"application_config":{"applications":[{"name":"der_dispatch_app","config_string":"{\"OPF\": 0, \"run_freq\": 60, \"run_on_host\": false, \"run_realtime\": false, \"stepsize_xp\": 0.2, \"stepsize_xq\": 2, \"coeff_p\": 0.1, \"coeff_q\": 5e-05, \"stepsize_mu\": 500}"}]},"simulation_request_type":"NEW"}' '{OPF:0,run_freq:60,run_on_host:false,run_realtime:false,stepsize_xp:0.2,stepsize_xq:2,coeff_p:0.1,coeff_q:5e-05,stepsize_mu:500}'
```



```json
{"OPF": 0, "run_freq": 60, "run_on_host": false, "run_realtime": true, "stepsize_xp": 0.2, "stepsize_xq": 2, "coeff_p": 0.1, "coeff_q": 5e-05, "stepsize_mu": 500}
```


