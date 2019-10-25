# Copyright (c) 2019 Alliance for Sustainable Energy, LLC
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import json
import time
from time import strptime, strftime, mktime, gmtime
from calendar import timegm
import argparse
from gridappsd import GOSS
from main_app_new import DER_Dispatch

from gridappsd import GridAPPSD
from gridappsd.topics import simulation_output_topic, simulation_log_topic

goss_sim = "goss.gridappsd.process.request.simulation"
test_topic = 'goss.gridappsd.test'
responseQueueTopic = '/temp-queue/response-queue'
goss_simulation_status_topic = '/topic/goss.gridappsd/simulation/status/'


def _startTest(username,password,gossServer='localhost',stompPort='61613', simulationID=1234, rulePort=5000, topic="input"):

    req_template = {"power_system_config": {"SubGeographicalRegion_name": "_1CD7D2EE-3C91-3248-5662-A43EFEFAC224",
                                            "GeographicalRegion_name": "_24809814-4EC6-29D2-B509-7F8BFB646437",
                                            "Line_name": "_C1C3E687-6FFD-C753-582B-632A27E28507"},
                    "simulation_config": {"power_flow_solver_method": "NR",
                                          "duration": 120,
                                          "simulation_name": "ieee123",
                                          "simulator": "GridLAB-D",
                                          "start_time": 1248156000,
                                          "run_realtime": True,
                                          "timestep_frequency": "1000",
                                          "timestep_increment": "1000",
                                          "model_creation_config": {"load_scaling_factor": 1.0, "triplex": "y",
                                                                    "encoding": "u", "system_frequency": 60,
                                                                    "voltage_multiplier": 1.0,
                                                                    "power_unit_conversion": 1.0, "unique_names": "y",
                                                                    "schedule_name": "ieeezipload", "z_fraction": "0",
                                                                    "i_fraction": "1", "p_fraction": "0",
                                                                    "randomize_zipload_fractions": False,
                                                                    "use_houses": False},
                                          "simulation_broker_port": 52798, "simulation_broker_location": "127.0.0.1"},
                    "application_config": {"applications": [{"name": "der_dispatch_app", "config_string": "{}"}]},
                    "simulation_request_type": "NEW", "test_config": {"events": []}}
    sw5_event = {
            "message": {
                "forward_differences": [
                    {
                        "object": "_60208A8D-E4EA-DA37-C758-428756C84F0D",
                        "attribute": "Switch.open",
                        "value": 1
                    }
                ],
                "reverse_differences": [
                    {
                        "object": "_60208A8D-E4EA-DA37-C758-428756C84F0D",
                        "attribute": "Switch.open",
                        "value": 0
                    }
                ]
            },
            "event_type": "ScheduledCommandEvent",
            "occuredDateTime": 1374510750,
            "stopDateTime": 1374510960
        }

    sw3_event = {
            "message": {
                "forward_differences": [
                    {
                        "object": "_4AA2369A-BF4B-F677-1229-CF5FB9A3A07E",
                        "attribute": "Switch.open",
                        "value": 1
                    }
                ],
                "reverse_differences": [
                    {
                        "object": "_4AA2369A-BF4B-F677-1229-CF5FB9A3A07E",
                        "attribute": "Switch.open",
                        "value": 0
                    }
                ]
            },
            "event_type": "ScheduledCommandEvent",
            "occuredDateTime": 1374258660 + (4*60),
            "stopDateTime": 1374258660 + (8*60)
        }
    event_l114 =  {"PhaseConnectedFaultKind": "lineToLineToGround",
                "FaultImpedance": {
                    "xGround": 0.36,
                    "rGround": 0.36,
                    "xLineToLine": 0.36,
                    "rLineToLine": 0.36
                },
                "ObjectMRID": ["_81CF3E64-ABA9-EF74-EE81-B86439ED61D5"], #  _ACA88F2A-96E3-B942-B09B-274CDD213CA6 PV no switches
                "phases": "ABC",
                "event_type": "Fault",
                "occuredDateTime": 1374258600 + (4*60),
                "stopDateTime":  1374258600 + (8*60)
    }
    event_1 = {
        "message": {
            "forward_differences": [
                {
                    "object": "_45990F5F-5FC3-4355-B2C9-435547E2CCA2", # l9191_48332_sw
                    "attribute": "Switch.open",
                    "value": 1
                }
            ],
            "reverse_differences": [
                {
                    "object": "_45990F5F-5FC3-4355-B2C9-435547E2CCA2",
                    "attribute": "Switch.open",
                    "value": 0
                }
            ]
        },
        "event_type": "ScheduledCommandEvent",
        "occuredDateTime": 1248174120,  # 2009-07-21 11:02:00 AM
        "stopDateTime": 1248174240      # 2009-07-21 11:04:00 AM
    }
    # 2009-07-21 05:00:00 AM
    # ##### event_1
    # Line LINE.LN5593236-6
    # node m1047515

    # "dg_84": "_233D4DC1-66EA-DF3C-D859-D10438ECCBDF", "dg_90": "_60E702BC-A8E7-6AB8-F5EB-D038283E4D3E"
    # Meas "_facde6ab-95e2-471b-b151-1b7125d863f0","_888e15c8-380d-4dcf-9876-ccf8949d45b1"

    # sx2991914c.1
    # new Line.2002200004991174_sw phases=3 bus1=d6290228-6_int.1.2.3 bus2=q16642.1.2.3 switch=y // CIM LoadBreakSwitch
    # ~ normamps=400.00 emergamps=600.00
    #   close Line.2002200004991174_sw 1
    # {'command': 'update', 'input': {'simulation_id': 966953393, 'message': {'timestamp': 1571850450, 'difference_mrid': 'caf85954-d594-42ec-b3d1-644a32941a4a', 'reverse_differences': [{'object': '_CB845255-3CD8-4E25-9B48-3CB74EE59F63', 'attribute': 'Switch.open', 'value': 1}], 'forward_differences': [{'object': '_CB845255-3CD8-4E25-9B48-3CB74EE59F63', 'attribute': 'Switch.open', 'value': 0}]}}}

    pv_84_90_event = {
        "allOutputOutage": False,
        "allInputOutage": False,
        "inputOutageList": [{"objectMRID": "_233D4DC1-66EA-DF3C-D859-D10438ECCBDF", "attribute": "PowerElectronicsConnection.p"},
                            {"objectMRID": "_233D4DC1-66EA-DF3C-D859-D10438ECCBDF", "attribute": "PowerElectronicsConnection.q"},
                            {"objectMRID": "_60E702BC-A8E7-6AB8-F5EB-D038283E4D3E", "attribute": "PowerElectronicsConnection.p"},
                            {"objectMRID": "_60E702BC-A8E7-6AB8-F5EB-D038283E4D3E", "attribute": "PowerElectronicsConnection.q"},
                            ],
        "outputOutageList": ['_a5107987-1609-47b2-8f5b-f91f99658390', '_2c4e0cb2-4bf0-4a2f-be94-83ee9b87d1e5'],
        "event_type": "CommOutage",
        # "occuredDateTime": 1374510780, #
        # "stopDateTime":  1374511260
        # "occuredDateTime": 1374510900,  # 35
        # "stopDateTime": 1374511080  # 38
        "occuredDateTime": 1374510600 + (5*60),
        "stopDateTime": 1374510600 + (10*60)
    }


    # {"applications": [{"name": "der_dispatch_app", "config_string": ""}]}
    req_template['simulation_config']['model_creation_config']['load_scaling_factor'] = 1
    req_template['simulation_config']['run_realtime'] = True
    req_template['simulation_config']['duration'] = 60 * 60 * 1
    req_template['simulation_config']['duration'] = 60 * 15

    req_template['simulation_config']['start_time'] = 1538510000
    req_template['simulation_config']['start_time'] = 1374498000  # GMT: Monday, July 22, 2013 1:00:00 PM What I was doing
    req_template['simulation_config']['start_time'] = 1374510600  # GMT: Monday, July 22, 2013 4:30:00 PM MST 10:30:00 AM
    # req_template['simulation_config']['start_time'] = 1374517800   # GMT: Monday, July 22, 2013 6:30:00 PM
    # req_template['simulation_config']['start_time'] = 1374510720  # GMT: Monday, July 22, 2013 4:30:00 PM PLUS 2 minutes!!
    # July 22, 2013 4:32:00 GMT
    # July 22, 2013 10:32:00 2013-07-22 10:32:00
    # req_template['simulation_config']['start_time'] = 1374514200  # GMT: Monday, July 22, 2013 5:30:00 PM

    # req_template['simulation_config']['start_time'] = 1374519600  # (GMT): Monday, July 22, 2013 7:00:00 PM
    # req_template['simulation_config']['start_time'] = 1374530400  # (GMT): Monday, July 22, 2013 10:00:00 PM Cause
    # req_template['simulation_config']['start_time'] = 1374454800  # (GMT): Monday, July 22, 2013 1:00:00 AM
    # req_template['simulation_config']['start_time'] = 1374411600  # 7/21/2013 7AM
    req_template['simulation_config']['start_time'] = 1374256800  # (GMT): Friday, July 19, 2013 6:00:00 PM
    req_template['simulation_config']['start_time'] = 1374217200  # July 19 07:00 AM GMT / 1:00 AM MST
    req_template['simulation_config']['start_time'] = 1374228000  # July 19 10:00 AM GMT / 4:00 AM MST
    req_template['simulation_config']['start_time'] = 1374233400  # July 19 11:30 AM GMT / 5:30 AM MST
    req_template['simulation_config']['start_time'] = 1374240600  # July 19 13:30 AM GMT / 7:30 AM MST
    req_template['simulation_config']['start_time'] = 1374213600  # July 19 06:00 AM GMT / 00:00 AM MST
    req_template['simulation_config']['start_time'] = 1374235200  # July 19 12:00 PM GMT / 06:00 AM MST
    req_template['simulation_config']['start_time'] = 1248156000  # Tuesday, July 21, 2009 6:00:00 AM
    req_template['simulation_config']['start_time'] = 1248192000  # Tuesday, July 21, 2009 4:00:00 PM / 10:00:00 AM
    req_template['simulation_config']['start_time'] = 1248199200  # Tuesday, July 21, 2009 6:00:00 PM / 12:00:00 PM
    req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 13:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 12:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    # req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 11:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    # req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 10:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    # req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 09:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    # req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 18:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    # req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 20:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    # req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 21:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    # req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 18:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    # req_template['simulation_config']['start_time'] = 1374249600  #  July 19, 2013 4:00:00 PM
    # req_template['simulation_config']['start_time'] = 1374258600  # July 19, 2013 6:30:00 PM  - 2013-07-19 18:30:00 / 12:30 PM MST  # MAX PV!!!
    # req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-19 12:30:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))  # July 19, 2013 6:30:00 PM  - 2013-07-19 18:30:00 / 12:30 PM MST  # MAX PV!!!
    # dg_42 _2B5D7749-6C18-D77E-B848-3F4C31ADC3E6 p=146621.68181873375 q=-179.94738632961975
    # 2013-07-19 18:32:00
    # 2013-07-19 18:35:00
    pv_84_90_event["occuredDateTime"] = req_template['simulation_config']['start_time'] + (5*60)
    pv_84_90_event["stopDateTime"]    = req_template['simulation_config']['start_time'] + (10*60)
    # req_template["test_config"]["events"].append(pv_84_90_event)

    # req_template["test_config"]["events"].append(sw3_event)
    # req_template["test_config"]["events"].append(event_l114)


    event_1["occuredDateTime"] = req_template['simulation_config']['start_time'] + (2*60)
    event_1["stopDateTime"]    = req_template['simulation_config']['start_time'] + (4*60)
    req_template["test_config"]["events"].append(event_1)

    app_config = {'OPF': 1, 'run_freq': 15, 'run_on_host': True}
    app_config['run_realtime'] = req_template['simulation_config']['run_realtime']
    app_config['stepsize_xp'] = 0.2
    app_config['stepsize_xq'] = 2
    # app_config['coeff_p'] = 0.1
    # app_config['coeff_q'] = 0.00005
    app_config['coeff_p'] = 0.005
    app_config['coeff_q'] = 0.005
    app_config['Vupper'] = 1.035
    app_config['Vlower'] = 0.96
    app_config['stepsize_mu'] = 50000
    app_config['optimizer_num_iterations'] = 15

    #TODO stepsize_mu = 50000 lower this! 500 or 50
    req_template["application_config"]["applications"] = [{"name": "der_dispatch_app", "config_string": json.dumps(app_config)}]

    # GMT: Tuesday, October 2, 2018 4:50:00 PM
    # Your time zone: Tuesday, October 2, 2018 10:50:00 AM GMT-06:00 DST
    req_template['power_system_config']['Line_name'] = '_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D'  # Mine 123pv'
    # req_template['power_system_config']['Line_name'] = '_EBDB5A4A-543C-9025-243E-8CAD24307380'  # 123 with reg
    # req_template['power_system_config']['Line_name'] = '_88B3A3D8-51CD-7466-5E29-B692F21723CB' # Mine with feet conv
    # req_template['power_system_config']['Line_name'] = '_DA00D94F-4683-FD19-15D9-8FF002220115'  # mine with house

    # req_template['power_system_config']['Line_name'] = '_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62'  # 13
    # req_template['power_system_config']['Line_name'] = '_AAE94E4A-2465-6F5E-37B1-3E72183A4E44'  # New 8500 9500
    # req_template['power_system_config']['Line_name'] = '_C1C3E687-6FFD-C753-582B-632A27E28507'
    # req_template['power_system_config']['Line_name'] = '_AAE94E4A-Jeff'
    # req_template['power_system_config']['Line_name'] = '_AAE94E4A-Jeff-off-gen'

    # req_template['power_system_config']['Line_name'] = '_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3'
    # req_template["application_config"]["applications"][0]['name'] = 'sample_app'
    req_template["application_config"]["applications"][0]['name'] = 'der_dispatch_app'

    # req_template['power_system_config']['Line_name'] = '_67AB291F-DCCD-31B7-B499-338206B9828F' # J1
    # req_template['power_system_config']['Line_name'] = '_4ACDF48A-0C6F-8C70-02F8-09AABAFFA2F5' # Mine

    simCfg13pv = json.dumps(req_template)
    print(simCfg13pv)

    goss = GOSS()
    goss.connect()

    simulation_id = goss.get_response(goss_sim, simCfg13pv, timeout=220) # 180 Maybe?
    simulation_id = int(simulation_id['simulationId'])
    print(simulation_id)
    print('sent simulation request')
    time.sleep(1)

    if app_config['run_on_host']:
        listening_to_topic = simulation_output_topic(simulation_id)
        log_topic = simulation_log_topic(simulation_id)
        model_mrid = req_template['power_system_config']['Line_name']
        start_time = req_template['simulation_config']['start_time']
        app_configs = req_template["application_config"]["applications"]
        app_config = [json.loads(app['config_string']) for app in app_configs if app['name'] == 'der_dispatch_app'][0]

        gapps = GridAPPSD(simulation_id)
        load_scale = req_template['simulation_config']['model_creation_config']['load_scaling_factor']
        der_0 = DER_Dispatch(simulation_id, gapps, model_mrid, './FeederInfo', start_time, app_config, load_scale)
        der_0.setup()
        gapps.subscribe(listening_to_topic, der_0)
        gapps.subscribe(log_topic, der_0)

        while der_0.running():
            time.sleep(0.1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t","--topic", type=str, help="topic, the default is input", default="input", required=False)
    parser.add_argument("-p","--port", type=int, help="port number, the default is 5000", default=5000, required=False)
    parser.add_argument("-i", "--id", type=int, help="simulation id", required=False)
    # parser.add_argument("--start_date", type=str, help="Simulation start date", default="2017-07-21 12:00:00", required=False)
    # parser.add_argument("--end_date", type=str, help="Simulation end date" , default="2017-07-22 12:00:00", required=False)
    # parser.add_argument('-o', '--options', type=str, default='{}')
    args = parser.parse_args()

    _startTest('system','manager',gossServer='127.0.0.1',stompPort='61613', simulationID=args.id, rulePort=args.port, topic=args.topic)
