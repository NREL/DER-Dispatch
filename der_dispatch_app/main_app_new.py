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

import argparse
import collections
from collections import defaultdict
import csv
from datetime import datetime, timezone
import json
import logging
import numpy as np
from tabulate import tabulate
import pandas as pd

import zmq
import zlib

from matplotlib import pyplot as plt
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
import os
import shutil
import sys
import signal
import time

from uuid import uuid4
sys.path.append('/usr/src/gridappsd-python')
from gridappsd import GridAPPSD, DifferenceBuilder
from gridappsd.topics import simulation_input_topic, simulation_output_topic, simulation_log_topic
input_from_goss_topic = '/topic/goss.gridappsd.fncs.input'
SIMULATION_STATUS_LOG_TOPIC = '/topic/goss.gridappsd.simulation.log'

from DER_Dispatch_base import DER_Dispatch_base
# import y_helper_functions
import query_model_adms as query_model
import ymatrix_function
import opt_function
from opt_function import RTOPF
import query_weather
from itertools import islice



# from der_dispatch_app.DER_Dispatch_base import DER_Dispatch_base
# import der_dispatch_app.y_helper_functions
# import der_dispatch_app.query_model_adms as query_model
# import der_dispatch_app.dss_function
# import der_dispatch_app.opt_function

use_dss_files = False
logging.basicConfig(filename='app'+str(use_dss_files)+'.log',
                    filemode='w',
                    # stream=sys.stdout,
                    level=logging.INFO,
                    format="%(asctime)s - %(name)s;%(levelname)s|%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
# Only log errors to the stomp logger.
logging.getLogger('stomp.py').setLevel(logging.ERROR)

_log = logging.getLogger(__name__)


class DER_Dispatch(DER_Dispatch_base):

    def __init__(self, simulation_id, gridappsd_obj, model_mrid, matrix_dir='./FeederInfo', start_time=1374498000, app_config={}, load_scale=1):
        """
        Constructor
        :param simulation_id:
        :param gridappsd_obj:
        :param model_mrid:
        :param matrix_dir:
        :param start_time:
        :param app_config:
        """
        _log.info("Init application")
        DER_Dispatch_base.__init__(self, None) # TODO redo this
        # if running_on_host():
        #     exit()
        self._is_running = True
        self.timestamp_ = 0
        self._vmult = .001
        self.slack_number = 3
        self.Tmax = 1440
        self.startH = 0  # in hour
        self.stepsize = 60
        self._gapps = gridappsd_obj
        self.fidselect = model_mrid
        self.ymatirx_dir = matrix_dir
        self.AllNodeNames = []
        self._start_time = start_time
        self._app_config = app_config
        self._RTOPF_flag = app_config.get('OPF', 1)
        self._run_freq = app_config.get('run_freq', 30)
        self._run_realtime = app_config.get('run_realtime',True)
        self._optimizer_num_iterations = app_config.get('optimizer_num_iterations',30)
        self._load_scale = load_scale

        # start_time = 1374498000 + (0 * 60 * 60)
        t = datetime.utcfromtimestamp(start_time)
        adjusted_time = int(t.replace(year=2013).replace(tzinfo=timezone.utc).timestamp())
        start_time = adjusted_time + (0 * 60 * 60)
        end_time = adjusted_time + 3600 * 24
        # self._weather_df = query_weather.query_weather(start_time , end_time )
        self._weather_df = query_weather.query_weather(start_time * 1000000, end_time * 1000000)
        # print(self._weather_df)

        self.sim_start = 0
        self._optgrid = None
        self.mu0 = []
        self.linear_PFmodel_coeff = 0

        self._PV_setpoints = defaultdict(lambda: {'timestamp': 0, 'p': 0., 'q': 0.})
        self._PV_setpoints['timestamp'] = 0
        self._check_pv_control_flag = False

        self.resFolder = ""
        self.res = {}
        self._results = {}
        self._PV_last_values = {}
        self.simulation_id = simulation_id
        self.present_step = 0
        self._last_toggle_on = False
        self._open_diff = DifferenceBuilder(simulation_id)
        self._close_diff = DifferenceBuilder(simulation_id)
        self._publish_to_topic = simulation_input_topic(simulation_id)
        self._send_simulation_status('STARTED', 'Initializing DER app for ' + str(self.simulation_id), 'INFO')
        self.MAX_LOG_LENGTH = 20

        self._switches = []
        self._switches_dict = {}
        self._PV_dict = {}
        self._load_dict = {}

        ctx = zmq.Context()
        self._skt = ctx.socket(zmq.PUB)
        if running_on_host():
            self._skt.bind('tcp://127.0.0.1:9001')
        else:
            self._skt.bind('tcp://*:9001')

        # signal.signal(signal.SIGCHLD, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def _send_simulation_status(self, status, message, log_level):
        """send a status message to the GridAPPS-D log manager

        Function arguments:
            status -- Type: string. Description: The status of the simulation.
                Default: 'localhost'.
            stomp_port -- Type: string. Description: The port for Stomp
            protocol for the GOSS server. It must not be an empty string.
                Default: '61613'.
            username -- Type: string. Description: User name for GOSS connection.
            password -- Type: string. Description: Password for GOSS connection.

        Function returns:
            None.
        Function exceptions:
            RuntimeError()
        """
        simulation_status_topic = "goss.gridappsd.process.simulation.log.{}".format(self.simulation_id)

        valid_status = ['STARTING', 'STARTED', 'RUNNING', 'ERROR', 'CLOSED', 'COMPLETE']
        valid_level = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
        if status in valid_status:
            if log_level not in valid_level:
                log_level = 'INFO'
            t_now = datetime.utcnow()
            status_message = {
                "source": os.path.basename(__file__),
                "processId": str(self.simulation_id),
                "timestamp": int(time.mktime(t_now.timetuple())),
                "processStatus": status,
                "logMessage": str(message),
                "logLevel": log_level,
                "storeToDb": True
            }
            status_str = json.dumps(status_message)
            _log.info("{}\n\n".format(status_str))
            # debugFile.write("{}\n\n".format(status_str))
            self._gapps.send(simulation_status_topic, status_str)

    def _send_pause(self):
        command = {
            "command": "pause"
        }
        command = json.dumps(command)
        _log.info("{}\n\n".format(command))
        self._gapps.send(self._publish_to_topic, command)

    def _send_resume(self):
        command = {
            "command": "resume"
        }
        command = json.dumps(command)
        _log.info("{}\n\n".format(command))
        self._gapps.send(self._publish_to_topic, command)

    def setup(self):
        if not self._run_realtime:
            self._send_pause()
            _log.info("Pausing to setup")

        _log.info("Get Y matrix")
        self._send_simulation_status('STARTED', 'Get Y matrix', 'INFO')
        ybus_config_request = {"configurationType": "YBus Export",
                               "parameters": {"simulation_id": 1659746927}}
        ybus_config_request['parameters']['simulation_id'] = str(self.simulation_id)
        #{
  # "configurationType":"YBus Export",
  # "parameters":{"simulation_id":"678841139"}
  # }
  #       time.sleep(5)

        if self.fidselect != '_AAE94E4A-2465-6F5E-37B1-3E72183A4E44_XXXXXXXX':
            'goss.gridappsd.process.request.config'
            ybus = self._gapps.get_response("goss.gridappsd.process.request.config", ybus_config_request, timeout=240)

            self.AllNodeNames = ybus['data']['nodeList']
            self.AllNodeNames = [node.replace('"','') for node in self.AllNodeNames]
            ybus_data = ybus['data']['yParse']

            # Old
            # self.AllNodeNames = ybus['data']['nodeListFilePath']
            # self.AllNodeNames = [node.replace('"','') for node in self.AllNodeNames]
            # ybus_data = ybus['data']['yParseFilePath']


        _log.info("NodeNames")
        _log.info(self.AllNodeNames)
        self._send_simulation_status('STARTED', "NodeNames " + str(self.AllNodeNames)[:200], 'INFO')

        # ******************************** Get System Information *********************************
        MainDir = os.getcwd()
        _log.info(MainDir)
        if 'der_dispatch_app' not in MainDir:
            MainDir = os.path.join(MainDir,'der_dispatch_app')
        FeederDir = os.path.join(MainDir, self.ymatirx_dir)
        if not os.path.exists(FeederDir):
            os.mkdir(FeederDir)


        if self.fidselect != '_AAE94E4A-2465-6F5E-37B1-3E72183A4E44_XXXXXX':
            Ysparse_file = os.path.join(FeederDir, 'base_no_ysparse_temp.csv')
            with open(Ysparse_file, 'w') as f:
                for item in ybus_data:
                    f.write("%s\n" % item)
        else:
            path_8500 = '/Users/jsimpson/git/adms/gridappsd-docker-upstream/DER-Dispatch-app/updated_glm2/173895120/'
            AllNodeNames8500 = []
            with open(os.path.join(path_8500, 'base_nodelist.csv')) as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                for row in reader:
                    AllNodeNames8500.append(row[0])
            self.AllNodeNames = AllNodeNames8500
            Ysparse_file = '/Users/jsimpson/git/adms/gridappsd-docker-upstream/DER-Dispatch-app/updated_glm2/173895120/base_ysparse.csv'
            _log.info("NodeNames")
            _log.info(self.AllNodeNames[:201])

        # self.AllNodeNames = [str(node) for node in self.AllNodeNames]
        self.node_number = len(self.AllNodeNames)
        self.Vbase_allnode = [0] * self.node_number
        self.all_node_index = {nn: count for count, nn in enumerate(self.AllNodeNames)}

        print(self.AllNodeNames[:200])
        # ii = 0
        # for node in self.AllNodeNames:
        #     self.circuit.SetActiveBus(node)
        #     self.Vbase_allnode[ii] = dss.Bus.kVBase() * 1000
        #     ii = ii + 1
        with open('result_nodename.csv', 'w') as f:
            csvwriter = csv.writer(f)
            for ii in range(self.node_number):
                csvwriter.writerow([self.AllNodeNames[ii]])
        f.close()

        # --------- Get Ymatrix and No Load Voltage before Doing OPF ------
        # if RTOPF_flag == 1:
            # Ysparse_file = os.path.join(os.getcwd(), str(self.circuit.Name()).upper() + '_SystemY.txt')
            # [self.Y00, self.Y01, self.Y10, self.Y11, Y11_sparse, self.Y11_inv] = dss_function.construct_Ymatrix(Ysparse_file, self.slack_number, self.node_number)
            # # dss.run_command('solve')
            # V_vector_noLoad = dss_function.get_Vcomplex_Yorder(self.circuit, self.node_number)

        YVA_file = os.path.join(FeederDir, 'base_no_va.csv')
        # Ysparse_file = os.path.join(FeederDir, 'base_no_ysparse_temp.csv')

        self._source_node_names = query_model.get_source_node_names(self.fidselect)
        self._source_dict = {}
        for sn in self._source_node_names:
            # index = self.AllNodeNames.index(sn)
            index = self.all_node_index[sn]
            self._source_dict[index] = {'name':sn, 'index':index}
        _log.info("Source slack")
        _log.info(self._source_dict )

        self.slack_start = sorted(self._source_dict.keys())[0]
        self.slack_end = sorted(self._source_dict.keys())[-1]

        _log.info("Get Y matrix")
        [self.Y00, self.Y01, self.Y10, self.Y11_inv] = \
            ymatrix_function.construct_Ymatrix_amds_slack_sparse(Ysparse_file, self.slack_number, self.slack_start, self.slack_end, self.node_number)

        _log.info(repr(self.Y00))

        self._send_simulation_status('STARTED', "Get Base Voltages", 'INFO')

        self.Vbase_allnode = ymatrix_function.get_base_voltages(self.AllNodeNames, self.fidselect)
        _log.info('Vbase_allnode')
        _log.info(self.Vbase_allnode[:500])

        self.result, self.name_map, self.node_name_map_va_power, self.node_name_map_pnv_voltage, \
        self.pec_map, self.load_power_map, self.line_map, self.trans_map, self.switch_pos, self.cap_pos, self.tap_pos,\
        self.load_voltage_map, self.line_voltage_map, self.trans_voltage_map = query_model.lookup_meas(self.fidselect)

        _log.info("Get Regulators")
        self._tap_name_map = {reg['bus'].upper() + '.' + query_model.lookup[reg['phs']]: reg['rname'] for reg in query_model.get_regulator(self.fidselect)}

        _log.info("Get Switches")
        self._switches = query_model.get_switches(self.fidselect)
        self._switch_name_map = {switch['bus1'].upper() + '.' + query_model.lookup[switch['phases'][0]]: switch['name'] for switch in self._switches}
        # for bn in self._switch_name_map.keys():
        #     print('meas for switch at bus ' + bn + ' ' + self.switch_pos[bn])

        _log.info("Get PVs")
        self.PVSystem = query_model.get_solar(self.fidselect)

        for load in self.PVSystem:
            for phase_index in range(load['numPhase']):
                phase_name = load['phases'][phase_index]
                # print(load['bus'].upper(), phase_name)
                name = load['bus'].upper() + '.' + query_model.lookup[phase_name]
                # index = self.AllNodeNames.index(name)
                index = self.all_node_index[name]
                # temp_index_list.append(index)
                # temp_PV_inverter_size.append(float(pv['kVA']))
                # temp_fdrid.append(pv['pecid'])
                meas_mrid = self.pec_map[name]
                self._PV_dict[index] = {'size': float(load['kVA']),
                                        'fmrid': load['pecid'],
                                        'id': load['id'],
                                        'name': load['name'],
                                        'busname': name,
                                        'meas_mrid': meas_mrid,
                                        'busphase': load['busphase'],
                                        'numPhase': load['numPhase'],
                                        'last_time': {'p': 0.0, 'q': 0.0},
                                        'current_time': {'p': 0.0, 'q': 0.0},
                                        'polar': {'p': 0.0, 'q': 0.0}
                                        }

        self._PV_dict = collections.OrderedDict(sorted(self._PV_dict.items()))
        self.NPV = len(self._PV_dict)

        # _log.info(json.dumps(self._PV_dict, indent=2))
        self.nodeIndex_withPV = list(self._PV_dict.keys())
        self.PV_inverter_size = [d[1]['size'] for d in self._PV_dict.items()]

        # INFO | [8, 31, 54, 79, 92, 112, 132, 145, 162, 176, 190, 208, 232, 245]
        # INFO | [120.0, 120.0, 250.0, 300.0, 400.0, 150.0, 250.0, 130.0, 260.0, 260.0, 280.0, 150.0, 300.0, 350.0]

        _log.info("PV Node indexes")
        _log.info(self.nodeIndex_withPV)
        _log.info("PV inverer sizes")
        _log.info(self.PV_inverter_size)

        self.Load, total_load = query_model.get_loads_query(self.fidselect,self._load_scale) #TODO scale

        for load in self.Load:
            for phase_index in range(load['numPhase']):
                phase_name = load['phases'][phase_index]
                name = load['bus'].upper() + '.' + query_model.lookup[phase_name]
                # index = self.AllNodeNames.index(name)
                index = self.all_node_index[name]
                meas_mrid = self.load_power_map[name]
                self._load_dict[index] = {'name': load['name'],
                                          'busname': name,
                                          'meas_mrid': meas_mrid,
                                          'busphase': load['busphase'],
                                          'numPhase': load['numPhase'],
                                          'phases': load['phases'],
                                          'constant_currents': load['constant_currents'],
                                          'phase': phase_name,
                                          'last_time': {'p': 0.0, 'q': 0.0},
                                          'current_time': {'p': 0.0, 'q': 0.0},
                                          'polar': {'p': 0.0, 'q': 0.0}}

        self._capacitors = query_model.get_capacitors(self.fidselect)
        _log.info("Get Capacitors")
        _log.info(self._capacitors)

        # --- Initialize the files to write results
        # circuit_name = "ieee123" # self.circuit.Name()
        circuit_name = query_model.get_feeder_name(self.fidselect)
        opts_str = ""
        # opts_off_str = ""
        opts = ['OPF', 'stepsize_xp', 'stepsize_xq', 'coeff_p', 'coeff_q', 'stepsize_mu']
        for k in opts:
            # if k == 'OPF':
            #     opts_off_str += k + '_0_'
            # else:
            #     opts_off_str += k + '_' + str(self._app_config[k]) + '_'
            opts_str += '_' + k + '_' + str(self._app_config[k])
        opts_str = opts_str.replace('.', '_').replace('-', '_neg_')
        # opts_off_str = opts_off_str.replace('.', '_').replace('-', '_neg_')
        self.resFolder = os.path.join(MainDir, 'adms_result_' + circuit_name + '_' + str(self._start_time)) + opts_str
        self.opf_off_folder = os.path.join(MainDir, 'adms_result_' + circuit_name + '_' + str(self._start_time)) + "_OPF_0_"
        if self._RTOPF_flag == 0:
            self.resFolder = self.opf_off_folder
        if self._RTOPF_flag == 1:
            _log.info("self.opf_off_folder")
            _log.info(self.opf_off_folder)
            self.PVoutput_P_df = pd.read_csv(os.path.join(self.opf_off_folder, "PVoutput_P.csv"), index_col='epoch time')
            # self.PVoutput_P_df.index = pd.to_datetime(self.PVoutput_P_df.index, unit='s')
            self.results_0_df = pd.read_csv(os.path.join(self.opf_off_folder, "result.csv"), index_col='epoch time')
            # self.results_0_df.index = pd.to_datetime(self.results_0_df.index, unit='s')

        if os.path.exists(self.resFolder):
            try:
                shutil.rmtree(self.resFolder)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))

        if not os.path.exists(self.resFolder):
            os.mkdir(self.resFolder)
        capNames = [cap['name'] for cap in self._capacitors]
        hCapNames = ','.join(capNames)

        headerStats = 'second,epoch time,solar_pct,GHI,Diff,Load Demand (MW),Load Demand (MVAr),' \
                      'Sub Power (MW),Sub Reactive Power (MVar),' \
                      'PVGeneration(MW),PVGeneration(MVAr),' \
                      'Vavg (p.u.),Vmax (p.u.),Vmin (p.u.),' + hCapNames + ',Active Losses(MW)'

        for key in headerStats.split(','):
            self._results[key] = 0.
        filename = os.path.join(self.resFolder,'result.csv')
        try:
            os.remove(filename)
        except:
            pass
        self._res_csvfile = open(filename, 'a')
        self._results_writer = csv.DictWriter(self._res_csvfile, fieldnames=headerStats.split(','), delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self._results_writer.writeheader()

        volt_file = os.path.join(self.resFolder, 'voltage.csv')
        if os.path.exists(volt_file):
            os.remove(volt_file)
        self.vn_file = open(volt_file, 'a')
        self.vn = csv.writer(self.vn_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self.vn.writerow(self.AllNodeNames)

        pv_names_index_order = [v['name'] for k, v in self._PV_dict.items()]
        pv_names_index_order.insert(0,'epoch time')
        filename = os.path.join(self.resFolder, 'PVoutput_P.csv')
        try:
            os.remove(filename)
        except:
            pass
        self._PV_P_csvfile = open(filename, 'a')
        self._PV_P_writer = csv.writer(self._PV_P_csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self._PV_P_writer.writerow(pv_names_index_order)

        filename = os.path.join(self.resFolder, 'PVoutput_Q.csv')
        try:
            os.remove(filename)
        except:
            pass
        self._PV_Q_csvfile = open(filename, 'a')
        self._PV_Q_writer = csv.writer(self._PV_Q_csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self._PV_Q_writer.writerow(pv_names_index_order)

        # Define control parameters
        self.control_bus = np.concatenate((self.AllNodeNames[:self.slack_start], self.AllNodeNames[self.slack_end+1:]))
        self.control_bus_index = [self.all_node_index[ii] for ii in self.control_bus]

        if not self._run_realtime:
            self._send_resume()
            _log.info("Resuming after setup")

    def running(self):
        return self._is_running

    def signal_handler(self, signal, frame):
        """
        Capture the CTRL+C signal and save everything.
        :param signal:
        :param frame:
        :return:
        """
        print('You pressed Ctrl+C! Saving output')
        self._res_csvfile.close()
        self.vn_file.close()
        self._PV_P_csvfile.close()
        self._PV_Q_csvfile.close()
        self._skt.close()
        self.save_plots()
        self._is_running = False
        sys.exit(0)

    def on_message(self, headers, message):
        """ Handle incoming messages on the fncs_output_topic for the simulation_id
        Parameters
        ----------
        headers: dict
            A dictionary of headers that could be used to determine topic of origin and
            other attributes.
        message: object
            A data structure following the protocol defined in the message structure
            of ``GridAPPSD``.  Most message payloads will be serialized dictionaries, but that is
            not a requirement.
        """
        # Check if we are getting messages
        # _log.info(message)

        # print(headers['destination'])
        # print(message[:100])
        _log.info('msg')
        _log.info(type(message))
        _log.info(str(message))
        _log.info(headers['destination'])

        if 'message' not in message:
            return
        if headers['destination'].startswith('/topic/goss.gridappsd.simulation.log.'+str(self.simulation_id)):
            if type(message) == str:
                message = json.loads(message)
            if message['processStatus'] == "COMPLETE":
                print(message)
                self.signal_handler(None, None)
            return
        # print(message[:100])

        self._send_simulation_status('STARTED', "Rec message " + str(message)[:100], 'INFO')

       # {'simulation_id': '927687385', 'message': {'timestamp': 1374510962, 'measurements': []}}
        if type(message) == str:
            message = json.loads(message)
        if self.timestamp_ == message['message']['timestamp']:
            # print("Measurements is duplicate")
            # _log.info("Measurements is duplicate")
            return

        self.timestamp_ = message['message']['timestamp']
        if not message['message']['measurements']:
            print("Measurements is empty")
            return
        self.present_step = self.present_step + 1

        if (self.timestamp_ - 2) % self._run_freq != 0:
            print('Time on the time check. ' + str(self.timestamp_) + ' ' + datetime.fromtimestamp(self.timestamp_, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
            return
        else:
            if not self._run_realtime:
                self._send_pause()
                _log.info("Pausing to work")

        meas_map = message['message']['measurements']
        with open('meas_map.json', 'w') as outfile:
            json.dump(meas_map, outfile, indent=2)
        # meas_map = query_model.get_meas_map(message)

        print("Get Tap Pos")
        taps = query_model.get_pos(meas_map, self.tap_pos)
        print(taps)

        print("Get Switches Pos")
        switch_pos = query_model.get_pos(meas_map, self.switch_pos)
        for name, value in self._switch_name_map.items():
            print(name, value, switch_pos[name])
        print(switch_pos)

        print("Get PQ Load")
        PQ_load = query_model.get_PQNode(self.AllNodeNames, meas_map, self.load_power_map, convert_to_radian=True)
        query_model.get_pv_PQ(self._load_dict, self.AllNodeNames, meas_map, self.load_power_map, convert_to_radian=True)
        PQ_load = np.array(PQ_load) / 1000.
        # PV CIM id, Meas ID, TermID,  Name,
        _log.info("PQ_load")
        _log.info(repr(PQ_load[:self.MAX_LOG_LENGTH]))

        # Maybe find a different way ...
        total_pq = self.get_demand(self.AllNodeNames, self._load_dict, self.load_power_map, self.load_voltage_map, meas_map)
        # total_pq = 0.0


        # print('PQ_load')
        # print(PQ_load.tolist())
        # PQ_load = np.array(Pload) + 1j * np.array(Qload)
        temp_load_dict = dict(islice(self._load_dict.items(), self.MAX_LOG_LENGTH))
        load_df = pd.DataFrame(temp_load_dict)
        load_df.loc['index'] = load_df.columns
        load_df.columns = [d[1]['name'] for d in temp_load_dict.items()]
        # print("load columns")
        # print(load_df.columns)
        _log.info("Load data \n" + tabulate(load_df, headers='keys', tablefmt='psql'))


        # PQ_node = query_model.get_PQNode(self.AllNodeNames, meas_map, node_name_map)
        ## TODO Need this?
        # PQ_node = - PQ_load + PQ_PV + 1j * np.array(Qcap)

        V_node = query_model.get_PQNode(self.AllNodeNames, meas_map, self.node_name_map_pnv_voltage, convert_to_radian=True)
        _log.info(V_node[:self.MAX_LOG_LENGTH])
        for index, v1 in enumerate(V_node):
            if v1.real == 0:
                # V_node[index] = complex(2424.0, 0.)
                V_node[index] = complex(self.Vbase_allnode[index], 0.)
                _log.info("Zero value for " + self.AllNodeNames[index])
                self._send_simulation_status('RUNNING', "Zero value for node " + self.AllNodeNames[index], 'WARN')
        # V_node = np.concatenate((V_node[:self.slack_start], V_node[self.slack_end+1:]))

        ## Test
        if "_006a5104-0576-496c-be3d-796fa407b974" in meas_map:
            print("PV meas " + str(meas_map["_006a5104-0576-496c-be3d-796fa407b974"]))
        if "_a13724e1-ddc4-4354-a8f9-d731ff110ebd" not in meas_map:
            print("No meas for " + "_a13724e1-ddc4-4354-a8f9-d731ff110ebd")
        else:
            print("Found meas for " + str(meas_map["_a13724e1-ddc4-4354-a8f9-d731ff110ebd"]))
        Ppv, Qpv, pv_mrids = query_model.get_pv_PQ(self._PV_dict, self.AllNodeNames, meas_map, self.pec_map, convert_to_radian=True)
        # print(repr(self.pec_map))
        # for pv_index in self.nodeIndex_withPV:
        #     PVmax.append(Ppv[pv_index] * -vmult)
        #     # pv_names.append(self.AllNodeNames[pv_index])
        #     temp_Qpv.append(Qpv[pv_index] * -vmult)
        t = datetime.utcfromtimestamp(self.timestamp_)
        adjusted_time = int(t.replace(year=2013).replace(tzinfo=timezone.utc).timestamp())
        ghi, solar_diff = self.get_ghi(adjusted_time - 30)
        # ghi, solar_diff = self.get_ghi(self.timestamp_ - 30)

        solar_pct = (ghi + solar_diff) / 100.
        magic_pct = .985
        magic_pct = 1.00096244
        magic_pct = 1.00096243689
        solar_pct *= magic_pct
        ghi_str = 'ghi, solar_diff, magic_pct, solar_pct ' + str(ghi) + " " + str(solar_diff) + " " + str(magic_pct) + " " + \
                  str(solar_pct) + ' at Time ' + str(self.timestamp_)
        print(ghi_str)
        _log.info(ghi_str)
        PVmax = [solar_pct * v['size'] for k, v in self._PV_dict.items()]

        if self._RTOPF_flag == 1:
            df_index = self.PVoutput_P_df.index.get_loc(self.timestamp_, method='nearest')
            temp_df = self.PVoutput_P_df.iloc[df_index]
            PVmax_opf_0 = [-v for k, v in temp_df.items()]
            PVmax = PVmax_opf_0
            _log.info("PVmax at " + str(self.timestamp_))

        ## Check for zero pv p
        current_pv = [v['current_time']['p'] for k, v in self._PV_dict.items()]
        PVname = np.array([v['name'] for k, v in self._PV_dict.items()])
        pv_zero_mask = np.array(current_pv) == 0
        if pv_zero_mask.sum() > 0:
            print('Check pv zero')
            print(repr(PVmax))
            print(repr(pv_zero_mask))
            PVmax = np.array(PVmax)
            PVmax[pv_zero_mask] = 0
            print(repr(PVmax))
            PVmax = PVmax.tolist()
            pct_pv_off = pv_zero_mask.sum() / len(PVmax)
            if pv_zero_mask.sum() > 0:
                _log.info('These PV are unavailable ' + repr(PVname[pv_zero_mask]))
            if pct_pv_off < .20:
                # _log.info('The percentage of pv that are unavailable ' + str(pv_zero_mask.sum()) + '/' + len(PVmax) + '=' + pct_pv_off)
                _log.info('The percentage of pv that are unavailable {}/{}={}'.format(pv_zero_mask.sum(),len(PVmax), pct_pv_off))


        self.validate_pv_setpoints(self.timestamp_)
        self.res = {}
        self.res['PV_Poutput'] = np.array([pv[1]['current_time']['p'] * self._vmult for pv in self._PV_dict.items()])
        self.res['PV_Qoutput'] = np.array([pv[1]['current_time']['q'] * self._vmult for pv in self._PV_dict.items()])
        if self.NPV > 0:
            temp_pv_dict = dict(islice(self._PV_dict.items(), self.MAX_LOG_LENGTH))
            pv_df = pd.DataFrame(temp_pv_dict)
            pv_df.loc['index'] = pv_df.columns
            pv_df.columns = [d[1]['name'] for d in temp_pv_dict.items()]
            print(tabulate(pv_df, headers='keys', tablefmt='psql'))
            _log.info("PV data \n" + tabulate(pv_df, headers='keys', tablefmt='psql'))
        else:
            print("No PVs!")
            _log.info("No PVs!")

        # V1_withoutOPF_pu = np.array(list(map(lambda x: abs(x[0]) / x[1], zip(V_node, self.Vbase_allnode))))
        # print('Test V1_withoutOPF_pu')
        # print(np.allclose(V1_withoutOPF_pu, abs(np.array(V_node)) / np.array(self.Vbase_allnode)))
        V1_withoutOPF_pu = abs(np.array(V_node)) / np.array(self.Vbase_allnode)
        self.vn.writerow(V1_withoutOPF_pu.tolist())

        # v_violation_mask = list(vv < 0.95 or vv > 1.05 for vv in V1_withoutOPF_pu)
        v_violation_mask = (V1_withoutOPF_pu < 0.95) | (V1_withoutOPF_pu > 1.05)
        violators = list(zip(np.array(self.AllNodeNames)[v_violation_mask], np.array(V_node)[v_violation_mask],
                 self.Vbase_allnode[v_violation_mask],V1_withoutOPF_pu[v_violation_mask]))
        _log.info('violators ')
        _log.info(repr(violators))

        # self.save_results(self.present_step)
        self._results['second'] = self.present_step
        self._results['epoch time'] = self.timestamp_
        self._results['solar_pct'] = solar_pct
        self._results['GHI'] = ghi
        self._results['Diff'] = solar_diff
        if self._RTOPF_flag == 1:
            self.OPF(PQ_load, PVmax, None, V1_withoutOPF_pu, V_node, self.timestamp_)


        for cap in self._capacitors:
            for index in range(cap["numPhase"]):
                node_name = cap["busname"].upper() + '.' + cap["busphase"][index]
                cap['current_time'] = meas_map[self.cap_pos[node_name]]['value']
                self._results[cap['name']] = meas_map[self.cap_pos[node_name]]['value']  # TODO each phase?

        # # source = self._sub_source[0]
        # source_total = complex(0, 0)
        # for source in self._sub_source:
        #     for index in range(source["numPhase"]):
        #         node_name = source["busname"].upper() + '.' + source["busphase"][index]
        #         if node_name in self.line_map:
        #             term = self.line_map[node_name][0]
        #             source_total += complex(meas_map[term][u'magnitude'], meas_map[term][u'angle'])
        #         elif node_name in self.trans_map:
        #             term = self.trans_map[node_name]
        #             source_total += complex(meas_map[term][u'magnitude'], meas_map[term][u'angle'])
        #         else:
        #             print("Figure out how to do this with transformer ends.")
        _log.info("Jeff 1")
        source_total = complex(0, 0)
        # TODO make function
        # for node_name in self._source_node_names:
        #     if node_name in self.line_map:
        #         term = self.line_map[node_name][0]
        #         source_total += complex(meas_map[term][u'magnitude'], meas_map[term][u'angle'])
        #     elif node_name in self.trans_map:
        #         term = self.trans_map[node_name]
        #         temp_source_meas = complex(*query_model.pol2cart(meas_map[term]['magnitude'], meas_map[term]['angle']))
        #         source_total += temp_source_meas
        #         # source_total += complex(meas_map[term][u'magnitude'], meas_map[term][u'angle'])
        #         print(node_name, temp_source_meas)
        #     else:
        #         print("Figure out how to do this with transformer ends.")
        # source_total /= 1000000.

        _log.info("Jeff 2")
        # self._results['Load Demand (MW)'] = sum(PQ_load).real * vmult * vmult
        # self._results['Load Demand (MVAr)'] = sum(PQ_load).imag * vmult * vmult
        self._results['Load Demand (MW)'] = total_pq.real * self._vmult * self._vmult
        self._results['Load Demand (MVAr)'] = total_pq.imag * self._vmult * self._vmult
        self._results['Sub Power (MW)'] = source_total.real * self._vmult * self._vmult
        self._results['Sub Reactive Power (MVar)'] = source_total.imag * self._vmult * self._vmult
        self._results['PVGeneration(MW)'] = sum(Ppv) * self._vmult
        self._results['PVGeneration(MVAr)'] = sum(Qpv) * self._vmult
        self._results['Vavg (p.u.)'] = sum(V1_withoutOPF_pu) / self.node_number
        self._results['Vmax (p.u.)'] = max(V1_withoutOPF_pu)
        self._results['Vmin (p.u.)'] = min(V1_withoutOPF_pu)
        self._results_writer.writerow(self._results)
        self._res_csvfile.flush()

        self._PV_P_writer.writerow(np.insert(self.res['PV_Poutput'], 0, self.timestamp_))
        self._PV_Q_writer.writerow(np.insert(self.res['PV_Qoutput'], 0, self.timestamp_))

        _log.info("Jeff 2")
        # temp_dict = dict()
        # temp_dict[u'GHI'] = {'data': ghi_obs, 'time': epoch_time}
        # temp_dict[u'Forecast GHI'] = {'data': final_, 'time': epoch_time + (60 * 30)}
        result0_df = defaultdict(float)
        if self._RTOPF_flag == 1:
            result0_df = self.results_0_df .iloc[self.results_0_df .index.get_loc(self.timestamp_, method='nearest')]

        plot_time = datetime.fromtimestamp(self.timestamp_, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        plot_time = self.timestamp_

        self.send_plots(ghi, plot_time, result0_df, solar_diff, source_total, meas_map)

        print('**************** time step= ' + str(self.present_step) + ' : ' + str(
            self.timestamp_) + '***************')
        _log.info('**************** time step= ' + str(self.present_step) + ' : ' + str(
            self.timestamp_) + '***************')
        if not self._run_realtime:
            self._send_resume()
            _log.info("Resuming to work")

    def send_plots(self, ghi, plot_time, result0_df, solar_diff, source_total, meas_map):
        obj = {}
        temp_dict = {}
        temp_dict[u'Load Demand (MW)'] = {'data': self._results['Load Demand (MW)'], 'time': plot_time}
        temp_dict[u'Sub Station (MW)'] = {'data': source_total.real, 'time': plot_time}
        obj[u'Load Demand (MW)'] = temp_dict
        temp_dict = {}
        temp_dict[u'Vmax'] = {'data': self._results['Vmax (p.u.)'], 'time': plot_time}
        temp_dict[u'Vmax OPF=0'] = {'data': result0_df['Vmax (p.u.)'], 'time': plot_time}
        temp_dict[u'Vavg'] = {'data': self._results['Vavg (p.u.)'], 'time': plot_time}
        temp_dict[u'Vavg OPF=0'] = {'data': result0_df['Vavg (p.u.)'], 'time': plot_time}
        temp_dict[u'Vmin'] = {'data': self._results['Vmin (p.u.)'], 'time': plot_time}
        temp_dict[u'Vmin OPF=0'] = {'data': result0_df['Vmin (p.u.)'], 'time': plot_time}
        obj[u'Voltages P.U.'] = temp_dict
        temp_dict = {}
        temp_dict[u'PVGeneration(MW)'] = {'data': self._results['PVGeneration(MW)'], 'time': plot_time}
        temp_dict[u'PVGeneration(MW) OPF=0'] = {'data': result0_df['PVGeneration(MW)'], 'time': plot_time}
        obj[u'PVGeneration(MW)'] = temp_dict
        temp_dict = {}
        temp_dict[u'PVGeneration(MVAr)'] = {'data': self._results['PVGeneration(MVAr)'], 'time': plot_time}
        temp_dict[u'PVGeneration(MVAr) OPF=0'] = {'data': result0_df['PVGeneration(MVAr)'], 'time': plot_time}
        obj[u'PVGeneration(MVar)'] = temp_dict
        temp_dict = {}
        temp_dict[u'DirectCH1'] = {'data': ghi, 'time': plot_time}
        temp_dict[u'Diffuse'] = {'data': solar_diff, 'time': plot_time}
        obj[u'Weather'] = temp_dict
        # print(repr(temp_dict))
        plot_range = len(self._PV_dict.items())
        if plot_range > 120:
            plot_range = 120
        for count, i in enumerate(range(0, plot_range, self.MAX_LOG_LENGTH)):
            last = i
            temp_pv_dict = dict(islice(self._PV_dict.items(), last, i+self.MAX_LOG_LENGTH))
            temp_dict_pv_p = {pv[1]['name']: {'data': pv[1]['current_time']['p'] * self._vmult, 'time': plot_time} for pv in
                         temp_pv_dict.items()}
            obj[u'Power P (KW) ' + str(count)] = temp_dict_pv_p
            temp_dict_pv_q = {pv[1]['name']: {'data': pv[1]['current_time']['q'] * self._vmult, 'time': plot_time} for pv in
                         temp_pv_dict.items()}
            obj[u'Power P (KVar) ' + str(count)] = temp_dict_pv_q

        tap_pos = query_model.get_pos(meas_map, self.tap_pos)
        temp_tap_dict = dict(islice(self._tap_name_map.items(), self.MAX_LOG_LENGTH))
        temp_dict = {pv[1]: {'data': tap_pos[pv[0]], 'time': plot_time} for pv in temp_tap_dict.items()}
        obj[u'Regulator Pos'] = temp_dict

        switch_pos = query_model.get_pos(meas_map, self.switch_pos)
        temp_switch_dict = dict(islice(self._switch_name_map.items(), self.MAX_LOG_LENGTH))
        temp_dict = {pv[1]: {'data': switch_pos[pv[0]], 'time': plot_time} for pv in temp_switch_dict.items()}
        obj[u'Switch Pos'] = temp_dict

        jobj = json.dumps(obj).encode('utf8')
        zobj = zlib.compress(jobj)
        print('zipped pickle is %i bytes' % len(zobj))
        self._skt.send(zobj)

    def validate_pv_setpoints(self, timestamp):
        if self._check_pv_control_flag:
            # Check after 5 seconds have passed
            # print(timestamp, self._PV_setpoints['timestamp'])
            if timestamp - self._PV_setpoints['timestamp'] < 6:
                return
            print('Checking controllable setpoints')
            MAX_CHECKING = 10
            for count, pv in enumerate(self._PV_dict.values()):
                current_p = pv['current_time']['p']
                current_q = pv['current_time']['q']
                if count < MAX_CHECKING:
                    print('Checking: ' + str(pv['name']) + '.p ' + str(current_p) + ' ' + str(self._PV_setpoints[pv['id']]['p']))
                    print('Checking: ' + str(pv['name']) + '.q ' + str(current_q) + ' ' + str(self._PV_setpoints[pv['id']]['q']))

                if count < MAX_CHECKING:
                    if abs(current_p - self._PV_setpoints[pv['id']]['p']) > 2.5 * 1000:
                        print('Control not set: ' + str(pv['name']) + '.p ' + str(current_p) + ' ' + str(
                            self._PV_setpoints[pv['id']]['p']))
                    if abs(abs(current_q) - abs(self._PV_setpoints[pv['id']]['q'])) > 5.5:
                        print('Control not set: ' + str(pv['name']) + '.q ' + str(current_q) + ' ' + str(
                            self._PV_setpoints[pv['id']]['q']))

            self._check_pv_control_flag = False

    def get_ghi(self, current_sim_time):
        # TODO get current GHI from platform
        current_sim_time = current_sim_time
        if self._weather_df is None:
            return 0.,0.
        temp_df = self._weather_df.iloc[self._weather_df.index.get_loc(current_sim_time, method='nearest')]
        # ghi = .662493
        # ghi = 0.9472
        return float(temp_df['DirectCH1']), float(temp_df['Diffuse'])

    def get_demand(self, nodenames, load_name_dict, load_powers, load_voltage_map, meas_map):
        load_results = {}
        total_pq = 0
        for mrid in load_powers.items():
            if mrid[1] in meas_map:
                # print(mrid[0] + " " + mrid[1] + " " + str(meas_map[mrid[1]]))
                # print(load_name_dict[mrid[1]])
                name = mrid[0]
                # index = nodenames.index(name)
                index = self.all_node_index[name]
                phase_name = load_name_dict[index]['phase']
                # meas_mrid = load_name_dict[index]
                # print(phase_name)
                if 's' in phase_name:
                    temp_voltage = meas_map[mrid[1]]
                    temp_voltage = complex(*query_model.pol2cart(temp_voltage['magnitude'], temp_voltage['angle']))
                    # print(mrid[1], temp_voltage)
                    p = temp_voltage
                    # p = meas_map[mrid[1]]
                    total_pq += p
                    # print(mrid[0] + " " + mrid[1] + " " + str(meas_map[mrid[1]]))
                    load_results[mrid[0]] = {'name': mrid[0], 'mrid': mrid[1], 'power': p}
                    # print(load_name_dict[mrid[1]])
            else:
                print("No MRID for " + str(mrid))
        for mrid in load_voltage_map.items():
            if mrid[1] in meas_map:
                # load = load_name_volt_dict[mrid[1]]
                name = mrid[0]
                index = nodenames.index(name)
                load = load_name_dict[index]
                phase_name = load['phase']
                # temp_voltage = meas_map[mrid[1]]
                # print("Demand voltage")
                # print(mrid[0], temp_voltage, phase_name)
                if 's' not in phase_name:
                    # print(load['constant_currents'])
                    constant_current = complex(load['constant_currents'][phase_name])
                    temp_voltage = meas_map[mrid[1]]
                    # print(mrid[0], temp_voltage)
                    temp_voltage_cart = complex(*query_model.pol2cart(temp_voltage['magnitude'], temp_voltage['angle']))
                    # print(mrid[1], temp_voltage_cart, phase_name)
                    p = temp_voltage['magnitude'] * constant_current.conjugate()
                    p = p * query_model.phase_shift[phase_name]
                    load_results[mrid[0]] = {'name': mrid[0], 'mrid': mrid[1], 'power': p}
                    # print(mrid[0] + " " + mrid[1] + " " + str(meas_map[mrid[1]]) + " " + str(constant_current) + " " + str(p))
                    total_pq += p
                else:
                    temp_voltage = meas_map[mrid[1]]
                    temp_voltage_cart = complex(*query_model.pol2cart(temp_voltage['magnitude'], temp_voltage['angle']))
                    load_results[mrid[0]]['voltage'] = temp_voltage_cart

        pv_df = pd.DataFrame(load_results)
        _log.info(tabulate(dict(islice(pv_df.items(), self.MAX_LOG_LENGTH)), headers='keys', tablefmt='psql'))
        _log.info(total_pq)
        print("Total demand " + str(total_pq) + " * scale " + str(self._load_scale) + " " + str(total_pq*self._load_scale))
        return total_pq

    def OPF(self, PQ_load, PVmax, Qcap, V1_withoutOPF_pu, Vbus, timestamp):
        """
        Process data for the Optimal Power Flow
        :param PQ_load:
        :param PVmax:
        :param Qcap:
        :param V1_withoutOPF_pu:
        :param Vbus:
        :return:
        """
        _log.info("Jeff OPF 1")
        # check_voltage = not all(vv >= 0.95 and vv <= 1.05 for vv in V1_withoutOPF_pu)
        # print(check_voltage)
        check_voltage = True
        if check_voltage:
            opt_count = 0

            # x0 = np.concatenate((PVmax, [0] * self.NPV))  # in kW/kVar
            # print('x0 first')
            # print(x0)
            # V0 = V1_withoutOPF_pu
            # get linear power flow model coefficients at the present time step
            # nodename = self.circuit.YNodeOrder()
            # Vbus = dss_function.get_Vcomplex_Yorder(self.circuit, self.node_number)
            # Vbus = V_node

            # V1 = ymatrix_function.re_orgnaize_for_volt_dict(Vbus, self.all_node_index, self.AllNodeNames)
            # print('V1,Vbus')
            # print(np.allclose(np.array(V1), np.array(Vbus)))
            V1 = Vbus

            # _log.info('V1')
            # _log.info(repr(V1))
            # np.savetxt('V1.out', np.array(V1), delimiter=',')
            # np.savez_compressed('V1_123', V1=V1)


            # v1_pu = list(map(lambda x: abs(x[0]) / x[1], zip(V1, self.Vbase_allnode)))
            if self.sim_start == 0:  # TODO or if topo change
                # (Y00, Y01, Y10, Y11_inv, V1, slack_number)

                self.linear_PFmodel_coeff = opt_function.linear_powerflow_model_slack_sparse_1(self.Y00, self.Y01, self.Y10, self.Y11_inv, V1,
                                                                        self.slack_number, self.slack_start, self.slack_end)
                # np.savez_compressed('Y_matrix', Y00=self.Y00, Y01=self.Y01, Y10=self.Y10, Y11_inv=self.Y11_inv, V1=V1,
                #                     slack_number=self.slack_number, slack_start=self.slack_start, slack_end=self.slack_end)
                _log.info("OPF 1 linear_powerflow_model_slack_sparse_1")

                # [coeff_V, coeff_Vm, coeff_Vmag_P, coeff_Vmag_Q, coeff_Vmag_k, coeff_sub_p, coeff_sub_q,
                #  coeff_sub_g] = opt_function.linear_voltage_model_slack(self.Y00, self.Y01, self.Y10, self.Y11_inv, [], V1,
                #                                                         self.slack_number, self.slack_start, self.slack_end)

                self.mu0 = []
                self.mu0.append(np.zeros(len(self.AllNodeNames)-self.slack_number))
                self.mu0.append(np.zeros(len(self.AllNodeNames)-self.slack_number))

                _log.info("Jeff OPF 2")
                PVname = []
                PVlocation = []
                PVsize = []
                invertersize = []
                for pv in self._PV_dict.values():
                    PVname.append(pv['name'])
                    PVlocation.append(pv['busname'])
                    # PVsize.append(float(pv['kW']))
                    # invertersize.append(float(pv['kVA']))
                    PVsize.append(float(pv['size'])) # TODO check
                    invertersize.append(float(pv['size']))
                _log.info("Jeff OPF 3")
                # print('invertersize')
                # print(invertersize)
                #
                #
                # print('control_bus_index')
                # print(self.control_bus_index)
                # print('self.nodeIndex_withPV')
                # print(self.nodeIndex_withPV)
                np.savez_compressed('optgrid', PVname=PVname, PVlocation=PVlocation,
                                    PVsize=PVsize, invertersize=invertersize, control_bus_index=self.control_bus_index
                                    , nodeIndex_withPV= self.nodeIndex_withPV)
                self._optgrid = RTOPF(PVname, PVlocation, PVsize, invertersize, 'controlBus',  self.control_bus_index, self.nodeIndex_withPV)
                coeff_PF = self._optgrid.system_topology_identifier(self.linear_PFmodel_coeff)
                _log.info("Jeff OPF 4")

                self.sim_start = 1
                return


            # ------ apply the setpoint ------
            PVpower = []
            # TODO Add forward - rev for platform PV setpoints
            for pv in self._PV_dict.values():
                PVpower.append(np.array([pv['current_time']['p'], pv['current_time']['q']]))

            # print('PVpower')
            # print(repr(PVpower))
            # print('PVmax')
            # print(PVmax)
            # Vmes = np.concatenate((Vbus[:self.slack_start], Vbus[self.slack_end + 1:]))
            Vmes = np.concatenate((V1_withoutOPF_pu[:self.slack_start], V1_withoutOPF_pu[self.slack_end + 1:]))

            for i in range(self._optimizer_num_iterations):
                [mu1, self.linear_PFmodel_coeff] = self._optgrid.coordinator(self.mu0, Vmes, self.linear_PFmodel_coeff, self._app_config)
                # np.savez_compressed('coordinator_args', mu0=self.mu0, Vmes=Vmes, PVpower=PVpower, PVmax=PVmax)
                x1, results = self._optgrid.DER_optimizer(self.linear_PFmodel_coeff, mu1, PVpower, PVmax, self.slack_start, self.slack_number, self._app_config)
                _log.info(results)
                self.mu0 = mu1

            # Need matching order MRIDs of PVS
            pv_totals = defaultdict(lambda: {'total_p': 0, 'total_q': 0, 'total_last_p': 0, 'total_last_q': 0})
            self._check_pv_control_flag = True
            for count, pv in enumerate(self._PV_dict.values()):
                p = x1[count]  # KW
                q = x1[count + self.NPV]  # kvar
                # last_p = pv['last_time']['p']
                # last_q = pv['last_time']['q']
                pv['last_time']['p'] = pv['current_time']['p']
                pv['last_time']['q'] = pv['current_time']['q']
                last_p = pv['last_time']['p']
                last_q = pv['last_time']['q']

                # total_p =0
                # for i in range(pv['numPhase']):
                pv_totals[pv['id']]['polar'] = pv['polar']
                pv_totals[pv['id']]['name'] = pv['name']
                pv_totals[pv['id']]['total_p'] += p
                pv_totals[pv['id']]['total_q'] += q
                pv_totals[pv['id']]['total_last_p'] += last_p
                pv_totals[pv['id']]['total_last_q'] += last_q
            inverter_diff = DifferenceBuilder(self.simulation_id)

            # dg_42 = '_2B5D7749-6C18-D77E-B848-3F4C31ADC3E6'
            # dg_42 = '_88B39F80-3BE8-2BFC-50BA-4D75604810D2'
            # # edit Generator.dg_42 kW=144.37569 kvar=-7.89116
            # p, q = 144.37569 * -1000., -7.89116 * -1000.
            # print('dg_42 ' + str(p) + ' ' + str(q))
            # p, q = query_model.cart2pol(p, q)
            # print('dg_42 ' + str(p) + ' ' + str(q))
            # inverter_diff.add_difference(dg_42, "PowerElectronicsConnection.p", p, p)
            # inverter_diff.add_difference(dg_42, "PowerElectronicsConnection.q", q, q)

            # object inverter {
            #   name "inv_pv_dg_42";
            #   parent "55_pvmtr";
            #   phases AN;
            #   generator_status ONLINE;
            #   four_quadrant_control_mode CONSTANT_PQ;
            #   inverter_type FOUR_QUADRANT;
            #   inverter_efficiency 1.0;
            #   power_factor 1.0;
            #   V_base 2400.000;
            #   rated_power 150000.000;
            #   P_Out 150000.000;
            #   Q_Out 0.000;
            #   object solar {
            #     name "pv_dg_42";
            #     generator_mode SUPPLY_DRIVEN;
            #     generator_status ONLINE;
            #     panel_type SINGLE_CRYSTAL_SILICON;
            #     efficiency 0.2;
            #     rated_power 150000.000;
            #   };
            # }
            for pv_id, pv in pv_totals.items():
                print("Set forward diff " + pv['name'] + ' ' + pv_id + ' p=' + str(pv['total_p']) + ' q=' + str(pv['total_q']))
                p = pv['total_p'] * -1000.
                q = pv['total_q'] * -1000.
                self._PV_setpoints['timestamp'] = timestamp
                self._PV_setpoints[pv_id] = {'p': p, 'q': q}
                # print(self._PV_setpoints)
                # p, q = query_model.cart2pol(p, q)
                p *= -1
                q *= -1
                _log.info("Set forward diff " + pv['name'] + ' ' + pv_id + ' p=' + str(p) + ' q=' + str(q))

                last_p = pv['polar']['p']
                last_q = pv['polar']['q']
                # last_p = pv['total_p']
                # last_q = pv['total_q']

                # print(pv['name'] + '.p ' + str(p) + ' ' + str(last_p) + ' diff ' + str((p - last_p)))
                # print(pv['name'] + '.q ' + str(q) + ' ' + str(last_q) + ' diff ' + str((q - last_q)))
                inverter_diff.add_difference(pv_id, "PowerElectronicsConnection.p", p, last_p)
                inverter_diff.add_difference(pv_id, "PowerElectronicsConnection.q", q, last_q)

            msg = inverter_diff.get_message()
            # msg = self.get_message(inverter_diff)
            # msg = inverter_diff.get_message()
            # print (msg)
            _log.info("Send PV diff msg")
            _log.info(msg)

            self._gapps.send(self._publish_to_topic, json.dumps(msg))


    def get_message(self, diff):
        msg = dict(command="update",
                   input=dict(simulation_id=diff._simulation_id,
                              message=dict(timestamp=int(time.time()),  #."2018-01-08 13:27:00.000Z",
                                           difference_mrid=str(uuid4()),
                                           reverse_differences=diff._reverse,
                                           forward_differences=diff._forward)))
        return msg.copy()

    def save_plots(self):
        """
        Save plots for comparison. Need to have OPF off run first and the OPF on for comparison.
        :return:
        """
        print("Saving plots to " + self.resFolder)
        results0 = pd.read_csv(os.path.join(self.opf_off_folder, "result.csv"), index_col='epoch time')
        results0.index = pd.to_datetime(results0.index, unit='s')
        if not os.path.exists(self.opf_off_folder):
            print('No unoptimized results to compare with')
            _log.info('No unoptimized results to compare with')
            return
        results1 = pd.read_csv(os.path.join(self.resFolder, "result.csv"), index_col='epoch time')
        results1.index = pd.to_datetime(results1.index, unit='s')
        size = (10, 10)
        fig, ax = plt.subplots(figsize=size)
        plt.grid(True)
        ax.plot(results0[[u'Vmax (p.u.)']])
        ax.plot(results1[[u'Vmax (p.u.)']])
        ax.plot(results0[[u'Vavg (p.u.)']])
        ax.plot(results1[[u'Vavg (p.u.)']])
        ax.plot(results0[[u'Vmin (p.u.)']])
        ax.plot(results1[[u'Vmin (p.u.)']])
        ax.legend(
            ['Vmax (p.u.) OPF=0', 'Vmax (p.u.) OPF=1', 'Vavg (p.u.) OPF=0', 'Vavg (p.u.) OPF=1', 'Vmin (p.u.) OPF=0',
             'Vmin (p.u.) OPF=1'])
        fig.savefig(os.path.join(self.resFolder, "Vmax"), bbox_inches='tight')

        fig, ax = plt.subplots(figsize=size)
        plt.grid(True)
        # ax.plot(results1[[u'solar_pct']] * 3320.0 * -1)
        ax.plot(results0[[u'PVGeneration(MW)']])
        ax.plot(results1[[u'PVGeneration(MW)']])
        ax.legend(['PV MW OPF=0', 'PV MW OPF=1'])

        # plt.show()
        fig.savefig(os.path.join(self.resFolder, "PV_Generation_MW"), bbox_inches='tight')

        fig, ax = plt.subplots(figsize=size)
        plt.grid(True)
        # ax.plot(results1[[u'solar_pct']] * 3320.0 * -1)
        ax.plot(results0[[u'PVGeneration(MVAr)']])
        ax.plot(results1[[u'PVGeneration(MVAr)']])
        ax.legend(['PV MVar OPF=0', 'PV MVar OPF=1'])

        # plt.show()
        fig.savefig(os.path.join(self.resFolder, "PV_Generation_MVar"), bbox_inches='tight')

        fig, ax = plt.subplots(figsize=size)
        plt.grid(True)
        ax.plot(results0[[u'Load Demand (MVAr)']])
        ax.plot(results1[[u'Load Demand (MVAr)']])
        ax.legend(['Load Demand (MVar) OPF=0', 'Load Demand (MVar) OPF=1'])
        fig.savefig(os.path.join(self.resFolder, "Load_Demand_MVar"), bbox_inches='tight')

        fig, ax = plt.subplots(figsize=size)
        plt.grid(True)
        ax.plot(results0[[u'Load Demand (MW)']])
        ax.plot(results1[[u'Load Demand (MW)']])
        ax.legend(['Load Demand (MW) OPF=0', 'Load Demand (MW) OPF=1'])

        # plt.show()
        fig.savefig(os.path.join(self.resFolder, "Load_Demand_MW"), bbox_inches='tight')



def _main_local():
    import opendssdirect as dss
    exit(0)
    simulation_id =1543123248
    listening_to_topic = simulation_output_topic(simulation_id)
    sim_request = None
    # model_mrid = sim_request["power_system_config"]["Line_name"]
    model_mrid = '_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D'
    model_mrid = '_4ACDF48A-0C6F-8C70-02F8-09AABAFFA2F5'
    # model_mrid = '_67AB291F-DCCD-31B7-B499-338206B9828F' # J1

    gapps = GridAPPSD(simulation_id)
    # __GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
    # gapps = GridAPPSD(simulation_id, address=__GRIDAPPSD_URI__)
    der_0 = DER_Dispatch(simulation_id, gapps, model_mrid, './IEEE13')
    der_0.setup()
    # gapps.subscribe(listening_to_topic, der_0)
    der_0.sim()
    der_0.process_results()

    while True:
        time.sleep(0.1)


def _main():
    from gridappsd import utils
    _log.info("Starting application")
    _log.info("Run local only -JEFF")
    _log.info("Args ")
    for arg in sys.argv[1:]:
        _log.info(type(arg))
        _log.info(arg)

    parser = argparse.ArgumentParser()
    parser.add_argument("simulation_id",
                        help="Simulation id to use for responses on the message bus.")
    parser.add_argument("request",
                        help="Simulation Request")
    parser.add_argument("opt",
                        help="opt")

    opts = parser.parse_args()
    _log.info(opts)
    listening_to_topic = simulation_output_topic(opts.simulation_id)
    log_topic = simulation_log_topic(opts.simulation_id)
    print(listening_to_topic)
    print(log_topic)

    sim_request = json.loads(opts.request.replace("\'", ""))
    model_mrid = sim_request['power_system_config']['Line_name']
    start_time = sim_request['simulation_config']['start_time']
    app_config = sim_request["application_config"]["applications"]
    load_scale = sim_request['simulation_config']['model_creation_config']['load_scaling_factor']
    # app_config = [json.loads(app['config_string']) for app in app_config if app['name'] == 'der_dispatch_app'][0]
    app = [app for app in app_config if app['name'] == 'der_dispatch_app'][0]
    if app['config_string']:
        app_config = json.loads(app['config_string'].replace(u'\u0027', '"'))
    else:
        app_config = {'run_freq': 60, 'run_on_host': False}
    _log.info(app_config['run_on_host'])
    ## Run the docker container. WOOT!
    if 'run_on_host' in app_config and app_config['run_on_host']:
        exit(0)

    _log.info("Model mrid is: {}".format(model_mrid))
    gapps = GridAPPSD(opts.simulation_id, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())

    # gapps = GridAPPSD(opts.simulation_id)
    der_0 = DER_Dispatch(opts.simulation_id, gapps, model_mrid, './FeederInfo', start_time, app_config, load_scale)
    der_0.setup()
    gapps.subscribe(listening_to_topic, der_0)
    gapps.subscribe(log_topic, der_0)

    while der_0.running():
        time.sleep(0.1)

def running_on_host():
    __GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
    if __GRIDAPPSD_URI__ == 'localhost:61613':
        return True
    return False

if __name__ == '__main__':
    if running_on_host():
        _main_local()
    else:
        _main()


