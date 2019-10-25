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


import os
import numpy as np
import csv
# import y_helper_functions


class DER_Dispatch_base:
    def __init__(self, output_fn):
        self.output_fn = output_fn
        self.present_step = 0
        self.res_allVolt = []
        self.res_PV_Pout = []
        self.res_PV_Qout = []
        self.subKW = []
        self.subKVAr = []
        self.loadDemandKw = []
        self.loadDemandKvar = []
        self.pvGenerationKw = []
        self.pvGenerationKvar = []
        self.v = []
        self.vmin = []
        self.vmax = []
        self.vmean = []
        self.regPos = []
        self.capState = []
        self.losses = []
        self.PV_Ppower_output = []
        self.PV_Qpower_output = []
        self.buffer_count = 10
        self.slack_number = 6
        self.Tmax = 1440 * 2
        self.startH = 0  # in hour

        self.stepsize = 60

        # self.fidselect = '_67AB291F-DCCD-31B7-B499-338206B9828F'  # j1
        # self.ymatirx_dir = 'J1_feeder'

        # self.fidselect = '_40B1F0FA-8150-071D-A08F-CD104687EA5D'  # ieee123pv
        # self.ymatirx_dir = './IEEE123Bus'

        # self.fidselect = '_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D'  # ieee123
        # self.ymatirx_dir = './IEEE123BusFinal'
        # self.ymatirx_dir = '../notebooks/123bus/'

        # self.fidselect = '_4ACDF48A-0C6F-8C70-02F8-09AABAFFA2F5'  # ieee13
        # self.ymatirx_dir = './IEEE13'

    def ybus_setup(self):
        # get loads
        # get pvs
        # get y matrix
        # get nodes
        # self.node_names = query_model.get_node_names()
        # self.source = query_model.get_source()
        # self.pvs = query_model.get_pv_query()
        # self.loads = query_model.get_loads_query()
        # self.yMatirx = query_model.get_y_matirx()

        # TODO get feeder name

        # TODO request ymatrix, wait until we get it

        # request_ybus.request_ybus()

        # TODO read data files from directory that is returned
        # TODO run scripts for ymatix with out load and ymatrix with load
        # base_no_ysparse.csv will be the no load ymatirx and
        # base_load_ysparse.csv will be the load ymatrix

        # with open('opendsscmd_noload.txt', 'w') as out:
        #     out.writelines(y_helper_functions.no_load_txt)
        # with open('opendsscmd_load.txt', 'w') as out:
        #     out.writelines(y_helper_functions.no_load_txt)



        # self.lookup = {'A': '1', 'B': '2', 'C': '3', 'N':'4','S1':'1','S2':'2', 's1':'1','s2':'2', 's1\ns2': ['1','2'],'': '1.2.3'} # TODO s1 s12?
        # current_dir = os.getcwd()
        # if 'der_dispatch_app' not in current_dir:
        #     current_dir = os.path.join(current_dir,'der_dispatch_app')
        # current_dir = os.path.join(current_dir, self.ymatirx_dir )
        # nodelist_file = os.path.join(current_dir, 'base_load_nodelist.csv')
        # Ysparse_file = os.path.join(current_dir,  'base_load_ysparse.csv')
        # self.AllNodeNames = y_helper_functions.get_nodelist(nodelist_file)
        # self.Ymatrix =y_helper_functions.construct_Ymatrix(Ysparse_file, self.AllNodeNames)

        pass

    def input(self, der_message_dict):
        """
        Updates the internal state of the feeder measurements being monitored
        and controlled from output from the GridAPPS-D simulator.
        """
        self.der_message_dict = der_message_dict

        #TODO get load p and q
        # Initialize regulator tap dict
        for reg_index in range(self.num_regs):
            self.reg_tap[self.reg_list[reg_index]] = [0] * 3        # 3-phase taps
        # Update regulator taps
        for reg_index in range(self.num_regs):
            self.reg_tap[self.reg_list[reg_index]][0] = self.vvc_message[self.simulation_name][self.reg_list[reg_index]]['tap_A']
            self.reg_tap[self.reg_list[reg_index]][1] = self.vvc_message[self.simulation_name][self.reg_list[reg_index]]['tap_B']
            self.reg_tap[self.reg_list[reg_index]][2] = self.vvc_message[self.simulation_name][self.reg_list[reg_index]]['tap_C']

    def output(self, PVsystem):
        """
        Collect all regulator and capacitor control actions and formulate the
        message dictionary to send to the GridAPPS-D simulator and pass that in
        as an argument to the function specified by output_fn.
        :return None.
        """
        self.output_dict = {}
        self.output_dict[self.simulation_name] = {}

        ## Set control values
        count = 0
        for pv in PVsystem:
            # dss.run_command('edit ' + str(pv["name"]) + ' Pmpp=' + str(x1[count]) + ' kvar=' + str(x1[count + NPV]))
            # Set PV setpoints
            pass

        self.output_fn(self.output_dict.copy())

    def process_results(self):

        print('\n********** Processing results ! ********************\n')
        nstep = int(np.ceil(self.Tmax-self.startH*60))
        last = self.present_step // self.buffer_count * self.buffer_count
        tseries = np.asarray(range(last, nstep))
        if self.loadDemandKvar:
            self.d = np.column_stack((tseries, np.asarray(self.loadDemandKw) / 1000, np.asarray(self.loadDemandKvar) / 1000,
                                 np.asarray(self.subKW) / 1000, np.asarray(self.subKVAr) / 1000, np.asarray(self.pvGenerationKw) / 1000,
                                 np.asarray(self.pvGenerationKvar) / 1000,
                                 np.asarray(self.vmean), np.asarray(self.vmax), np.asarray(self.vmin), np.asarray(self.capState),
                                 np.asarray(self.losses) / 1000)) #np.asarray(regPos),
            np.savetxt(self.fn, self.d, fmt='%.8e', delimiter=',')
        if self.v:
            self.d = np.row_stack(self.v)
        np.savetxt(self.vn, self.d, fmt='%.6e', delimiter=',')
        self.fn.flush()
        self.vn.flush()

        with open(os.path.join(self.resFolder,'PVoutput_P.csv'),'w') as f:
            csvwriter = csv.writer(f)
            csvwriter.writerows(self.PV_Ppower_output)

        with open(os.path.join(self.resFolder,'PVoutput_Q.csv'),'w') as f:
            csvwriter = csv.writer(f)
            csvwriter.writerows(self.PV_Qpower_output)