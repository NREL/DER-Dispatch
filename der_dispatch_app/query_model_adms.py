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

import math
import cmath
import numpy as np
import sys
import os
sys.path.append('/usr/src/gridappsd-python')
from gridappsd import GridAPPSD

prefix17 = '''
    PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c:  <http://iec.ch/TC57/2012/CIM-schema-cim17#>
    '''

prefix_cim100 = '''
PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX c:  <http://iec.ch/TC57/CIM100#>
'''

prefix16 = '''
    PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c:  <http://iec.ch/TC57/2012/CIM-schema-cim16#>'''

prefix=prefix_cim100
prefix17=prefix

vmult = 0.001
pos120 = complex (-0.5, 0.5 * cmath.sqrt(3.0)); # For Phase C
neg120 = complex (-0.5, -0.5 * cmath.sqrt(3.0)); # For Phase B
phase_shift = {'A':1,'B':neg120,'C':pos120,'s1':1,'s2':1}
lookup = {'A': '1', 'B': '2', 'C': '3', 'N':'4','S1':'1','S2':'2', 's1':'1','s2':'2', 's1\ns2': ['1','2'],'s2\ns1': ['2','1'],'': '1.2.3'}

__GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
if __GRIDAPPSD_URI__ == 'localhost:61613':
    gridappsd_obj = GridAPPSD(1234)
else:
    from gridappsd import utils
    # gridappsd_obj = GridAPPSD(simulation_id=1234, address=__GRIDAPPSD_URI__)
    print(utils.get_gridappsd_address())
    gridappsd_obj = GridAPPSD(1069573052, address=utils.get_gridappsd_address(),
                              username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())


# "SELECT ?name WHERE { ?s r:type c:Feeder. ?s c:IdentifiedObject.name ?name} ORDER by ?name"
def get_feeder(feeder_name="j1"):
    query = prefix17 + '''
    SELECT ?feeder_name ?fdrid WHERE {
                        ?s r:type c:Feeder. 
                        ?s c:IdentifiedObject.mRID ?fdrid.
                        ?s c:IdentifiedObject.name ?feeder_name
                        FILTER (?feeder_name="''' + feeder_name + '''")} 
                        ORDER by ?feeder_name
    '''
    results = gridappsd_obj.query_data(query)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
        print(b['feeder_name']['value'])
        print(b['fdrid']['value'])

def get_feeder_name(value):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"} """
    query = prefix17 + '''
    SELECT ?feeder_name WHERE {
                        ''' + fidselect + '''
                        ?s r:type c:Feeder. 
                        ?s c:IdentifiedObject.mRID ?fdrid.
                        ?s c:IdentifiedObject.name ?feeder_name
                        } 
                        ORDER by ?feeder_name
    '''
    results = gridappsd_obj.query_data(query)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
        # print(b['feeder_name']['value'])
        # print(b['fid']['value'])
        return b['feeder_name']['value']


def get_source(value):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"} """

    # feeder selection options - if all commented out, query matches all feeders

    query = prefix + '''
    SELECT DISTINCT ?name  ?fdrid ?bus ?basev ?nomv ?vmag ?vang ?r1 ?x1 ?r0 ?x0 ?trmid WHERE {
     ''' + fidselect + '''
  #VALUES ?fdrid {"_40B1F0FA-8150-071D-A08F-CD104687EA5D"}
#VALUES ?fdrid {"_C1C3E687-6FFD-C753-582B-632A27E28507"}  # 123 bus
#VALUES ?fdrid {"_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"}  # 13 bus
#VALUES ?fdrid {"_5B816B93-7A5F-B64C-8460-47C17D6E4B0F"}  # 13 bus assets
#VALUES ?fdrid {"_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3"}  # 8500 node
#VALUES ?fdrid {"_67AB291F-DCCD-31B7-B499-338206B9828F"}  # J1
#VALUES ?fdrid {"_9CE150A8-8CC5-A0F9-B67E-BBD8C79D3095"}  # R2 12.47 3
     ?s c:Equipment.EquipmentContainer ?fdr.
     ?fdr c:IdentifiedObject.mRID ?fdrid.
     ?s r:type c:EnergySource.
     ?s c:IdentifiedObject.name ?name.
     ?s c:ConductingEquipment.BaseVoltage ?bv.
     ?bv c:BaseVoltage.nominalVoltage ?basev.
     ?s c:EnergySource.nominalVoltage ?nomv. 
     ?s c:EnergySource.voltageMagnitude ?vmag. 
     ?s c:EnergySource.voltageAngle ?vang. 
     ?s c:EnergySource.r ?r1. 
     ?s c:EnergySource.x ?x1. 
     ?s c:EnergySource.r0 ?r0. 
     ?s c:EnergySource.x0 ?x0. 
     ?l r:type c:Feeder. 
     ?l c:IdentifiedObject.name ?feeder_name .
     ?t c:Terminal.ConductingEquipment ?s.
     ?t c:IdentifiedObject.mRID ?trmid.
     ?t c:Terminal.ConnectivityNode ?cn. 
     ?cn c:IdentifiedObject.name ?bus
    } ORDER BY ?name ?bus'''
    data = list()
    results = gridappsd_obj.query_data(query)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
        datum = dict()
        datum["name"] = b['name']['value']
        datum["fdrid"] = b['fdrid']['value']
        datum["busname"] = b['bus']['value']
        datum['busphase'] = ['1', '2', '3']
        datum["numPhase"] = len(datum['busphase'])
        datum["trmid"] = b['trmid']['value']
        datum["kV"] = float(b['basev']['value']) * vmult
        data.append(datum)
    return data


def get_solar(value='_67AB291F-DCCD-31B7-B499-338206B9828F'):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"} """
    query = prefix+'''
    SELECT ?name ?bus ?ratedS ?ratedU ?ipu ?p ?q ?fdrid ?id ?pecid (group_concat(distinct ?phs;separator="\\n") as ?phases) WHERE {
     ?s r:type c:PhotovoltaicUnit.
     ?s c:IdentifiedObject.name ?name.
     ?s c:IdentifiedObject.mRID ?id.
     ?pec c:PowerElectronicsConnection.PowerElectronicsUnit ?s.
     '''+ fidselect+'''
     ?pec c:IdentifiedObject.mRID ?pecid.
     ?pec c:Equipment.EquipmentContainer ?fdr.
     ?fdr c:IdentifiedObject.mRID ?fdrid.
     ?pec c:PowerElectronicsConnection.ratedS ?ratedS.
     ?pec c:PowerElectronicsConnection.ratedU ?ratedU.
     ?pec c:PowerElectronicsConnection.maxIFault ?ipu.
     ?pec c:PowerElectronicsConnection.p ?p.
     ?pec c:PowerElectronicsConnection.q ?q.
     OPTIONAL {?pecp c:PowerElectronicsConnectionPhase.PowerElectronicsConnection ?pec.
     ?pecp c:PowerElectronicsConnectionPhase.phase ?phsraw.
       bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs) }
     ?t c:Terminal.ConductingEquipment ?pec.
     ?t c:Terminal.ConnectivityNode ?cn.
     ?cn c:IdentifiedObject.name ?bus
    }
    GROUP by ?name ?bus ?ratedS ?ratedU ?ipu ?p ?q ?fdrid ?id ?pecid
    ORDER by ?name
    '''
    data = []
    results = gridappsd_obj.query_data(query)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
        phases = b['phases']['value'].split(',')
        # print(b['name']['value'])
        # print b['ipu']['value']
        # print b['p']['value']
        # print b['q']['value']
        # print b['ratedS']['value']
        # print b['ratedU']['value']
        # print b['bus']['value']
        # print b['fdrid']['value']
        # print b['phases']['value']
        if '\n' in phases[0]:
            phases = phases[0].split('\n')
        datum = dict()
        if phases == [u'']:
            datum['phases'] = ['A', 'B', 'C']
        else:
            datum['phases'] = phases
        if phases == [u'']:
            datum['busphase'] = ['1', '2', '3']
        else:
            datum['busphase'] = [lookup[phs] for phs in phases]

        datum["numPhase"] = len(datum['busphase'])
        # datum["numPhase"] = len(b['phases']['value'])
        # if len(b['phases']['value']) == 0:
            # datum["numPhase"] = 3
        datum["name"] = b['name']['value']
        datum["id"] = b['id']['value']
        datum["pecid"] = b['pecid']['value']
        datum["bus"] = b['bus']['value']
        datum["busname"] = b['bus']['value']
        datum["Pmpp"] = round( vmult * float(b['p']['value']) / datum["numPhase"], 2)
        datum["pf"] = 1.0 #?
#         datum["kV"] =  .120 #? vmult * float(b['basev']['value']) / cmath.sqrt(3.0)
        if datum["numPhase"] > 1:
            datum["kV"] = round(vmult * float(b['ratedU']['value']) / math.sqrt(3.0), 2)
        else:
            datum["kV"] = vmult * float(b['ratedU']['value'])
        datum["kVA"] = round(vmult * float(b['ratedS']['value']) / datum["numPhase"], 1)
        datum['power'] = [float(b['p']['value']) * vmult, float(b['q']['value']) * vmult]
        datum['p'] = float(b['p']['value']) * vmult
        datum['q'] = float(b['q']['value']) * vmult
        datum['ratedS'] = b['ratedS']['value']
        datum['ratedU'] = b['ratedU']['value']
#         datum["power"] = dss.CktElement.Powers()[0:2]

        data.append(datum)
    return data

def get_line_segements( value='_67AB291F-DCCD-31B7-B499-338206B9828F'):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"}
 ?s c:Equipment.EquipmentContainer ?fdr.
 ?fdr c:IdentifiedObject.mRID ?fdrid. """

    query = prefix + """SELECT ?name ?bus1 ?bus2 (group_concat(distinct ?phs;separator="\\n") as ?phases) ?eqid ?trm1id ?trm2id ?vnom1 WHERE {
    SELECT ?name ?bus1 ?bus2 ?phs ?eqid ?trm1id ?trm2id ?vnom1 WHERE {""" + fidselect + """
     ?s r:type c:ACLineSegment.
     ?s c:IdentifiedObject.name ?name.
     ?s c:IdentifiedObject.mRID ?eqid. 
     ?t1 c:Terminal.ConductingEquipment ?s.
     ?s c:ConductingEquipment.BaseVoltage ?lev. 
     ?lev c:BaseVoltage.nominalVoltage ?vnom1.
     ?t1 c:ACDCTerminal.sequenceNumber "1".
     ?t1 c:IdentifiedObject.mRID ?trm1id. 
     ?t1 c:Terminal.ConnectivityNode ?cn1. 
     ?cn1 c:IdentifiedObject.name ?bus1.
     ?t2 c:Terminal.ConductingEquipment ?s.
     ?t2 c:ACDCTerminal.sequenceNumber "2".
     ?t2 c:IdentifiedObject.mRID ?trm2id. 
     ?t2 c:Terminal.ConnectivityNode ?cn2. 
     ?cn2 c:IdentifiedObject.name ?bus2.
     OPTIONAL {?acp c:ACLineSegmentPhase.ACLineSegment ?s.
     ?acp c:ACLineSegmentPhase.phase ?phsraw.
        bind(strafter(str(?phsraw),\"SinglePhaseKind.\") as ?phs) } } ORDER BY ?name ?phs
     } GROUP BY ?name ?bus1 ?bus2 ?eqid ?trm1id ?trm2id ?vnom1
     ORDER BY ?name
    """
    data = list()
    results = gridappsd_obj.query_data(query,timeout=120)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
        # capacitors.append(p['id']['value'])
        phases = b['phases']['value'].split("\n")
        datum = dict()
        #         print (b['name']['value'],b['bus1']['value'],b['bus2']['value'],b['eqid']['value'],b['trm1id']['value'],b['trm2id']['value'], b['vnom1']['value'])
        #         print (b['phases']['value'])
        #         break
        if phases == [u'']:
            datum['phases'] = ['A', 'B', 'C']
        else:
            datum['phases'] = phases
        datum["numPhase"] = len(b['phases']['value'])
        if len(b['phases']['value']) == 0:
            datum["numPhase"] = 3
        datum['name'] = b['name']['value']
        datum['eqid'] = b['eqid']['value']
        datum['bus1'] = b['bus1']['value']
        datum['bus2'] = b['bus2']['value']
        datum['basekv'] = b['vnom1']['value']
        datum['trm1id'] = b['trm1id']['value']
        datum['trm2id'] = b['trm2id']['value']
        data.append(datum)
    return data

def is_secondary(pv):
#     print(pv['phases'][0].lower()[:])
    if pv['phases'][0].lower()[:2] == 's1' or pv['phases'][0].lower()[:2] == 's2':
        return True;
    return False

def get_high_side_tank(transformer_tanks):
    max_size = 0
    result = None
    for t in transformer_tanks:
        if float(t['basekv']) > max_size:
            result=t
            max_size=float(t['basekv'])
    return result

def get_pv_secondary_to_primary_map(fidselect):
    line_segs = get_line_segements(fidselect)
    line_segs_bus1 = {ls['bus1']:ls['bus2'] for ls in line_segs}
    line_segs_bus2 = {ls['bus2']:ls['bus1'] for ls in line_segs}
    pvs = get_solar(fidselect)
    trans_with_tanks = get_transformer_with_tanks(fidselect)
    trans_with_tanks_name_dict  = {}#  {t['name']+t:t for t in trans_with_tanks}
    for t in trans_with_tanks:
        if t['pmrid'] not in trans_with_tanks_name_dict:
            trans_with_tanks_name_dict[t['pmrid']] = []
        trans_with_tanks_name_dict[t['pmrid']].append(t)
    pv_secondary_to_primary_mapping = {}
    for pv in pvs:
        if is_secondary(pv):
    #         print(pv['bus'])
            bus_to_trans = pv['bus']
            if pv['bus'] in line_segs_bus1:
                bus_to_trans = line_segs_bus1[pv['bus']]
    #             print(line_segs_bus1[pv['bus']])
            if pv['bus'] in line_segs_bus2:
                bus_to_trans = line_segs_bus2[pv['bus']]
    #             print(pv['bus'], line_segs_bus2[pv['bus']])
    #         for ls in line_segs:
    #              pv['bus'] == ls[]
            for tran in trans_with_tanks:
                if bus_to_trans == tran['bus']:
#                     print(pv)
                    primary_tank = get_high_side_tank(trans_with_tanks_name_dict[tran['pmrid']])
                    for phase in pv['busphase']:
                        sec_bus=pv['bus'].upper()+'.'+phase
                        high_side_phase = primary_tank
                        datum = {'name': pv['name'],
                                'id':pv['id'],
                                'pv_bus':sec_bus,
                                'primary_bus':high_side_phase['bus']+'.'+lookup[high_side_phase['phases'][0]],
                                'primary_tank':primary_tank}
                        pv_secondary_to_primary_mapping[sec_bus]=datum
    return pv_secondary_to_primary_mapping

def get_regulator(value='_EBDB5A4A-543C-9025-243E-8CAD24307380'):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"} """
    query=prefix+'''
SELECT ?rname ?pname ?tname ?wnum ?phs ?incr ?mode ?enabled ?highStep ?lowStep ?neutralStep ?normalStep ?neutralU 
 ?step ?initDelay ?subDelay ?ltc ?vlim 
	?vset ?vbw ?ldc ?fwdR ?fwdX ?revR ?revX ?discrete ?ctl_enabled ?ctlmode ?monphs ?ctRating ?ctRatio ?ptRatio ?id ?fdrid ?bus
WHERE {
 ''' + fidselect + '''
 ?pxf c:Equipment.EquipmentContainer ?fdr.
 ?fdr c:IdentifiedObject.mRID ?fdrid.
 ?rtc r:type c:RatioTapChanger.
 ?rtc c:IdentifiedObject.name ?rname.
 ?rtc c:RatioTapChanger.TransformerEnd ?end.
 ?end c:TransformerEnd.Terminal ?trm.
 ?trm c:IdentifiedObject.mRID ?trmid.
 ?trm c:Terminal.ConnectivityNode ?cn. 
 ?cn c:IdentifiedObject.name ?bus.
 ?end c:TransformerEnd.endNumber ?wnum.
 OPTIONAL {?end c:TransformerTankEnd.phases ?phsraw.
  bind(strafter(str(?phsraw),"PhaseCode.") as ?phs)}
 ?end c:TransformerTankEnd.TransformerTank ?tank.
 ?tank c:TransformerTank.PowerTransformer ?pxf.
 ?pxf c:IdentifiedObject.name ?pname.
 ?tank c:IdentifiedObject.name ?tname.
 ?rtc c:RatioTapChanger.stepVoltageIncrement ?incr.
 ?rtc c:RatioTapChanger.tculControlMode ?moderaw.
  bind(strafter(str(?moderaw),"TransformerControlMode.") as ?mode)
 ?rtc c:TapChanger.controlEnabled ?enabled.
 ?rtc c:TapChanger.highStep ?highStep.
 ?rtc c:TapChanger.initialDelay ?initDelay.
 ?rtc c:TapChanger.lowStep ?lowStep.
 ?rtc c:TapChanger.ltcFlag ?ltc.
 ?rtc c:TapChanger.neutralStep ?neutralStep.
 ?rtc c:TapChanger.neutralU ?neutralU.
 ?rtc c:TapChanger.normalStep ?normalStep.
 ?rtc c:TapChanger.step ?step.
 ?rtc c:TapChanger.subsequentDelay ?subDelay.
 ?rtc c:TapChanger.TapChangerControl ?ctl.
 ?ctl c:TapChangerControl.limitVoltage ?vlim.
 ?ctl c:TapChangerControl.lineDropCompensation ?ldc.
 ?ctl c:TapChangerControl.lineDropR ?fwdR.
 ?ctl c:TapChangerControl.lineDropX ?fwdX.
 ?ctl c:TapChangerControl.reverseLineDropR ?revR.
 ?ctl c:TapChangerControl.reverseLineDropX ?revX.
 ?ctl c:RegulatingControl.discrete ?discrete.
 ?ctl c:RegulatingControl.enabled ?ctl_enabled.
 ?ctl c:RegulatingControl.mode ?ctlmoderaw.
  bind(strafter(str(?ctlmoderaw),"RegulatingControlModeKind.") as ?ctlmode)
 ?ctl c:RegulatingControl.monitoredPhase ?monraw.
  bind(strafter(str(?monraw),"PhaseCode.") as ?monphs)
 ?ctl c:RegulatingControl.targetDeadband ?vbw.
 ?ctl c:RegulatingControl.targetValue ?vset.
 ?asset c:Asset.PowerSystemResources ?rtc.
 ?asset c:Asset.AssetInfo ?inf.
 ?inf c:TapChangerInfo.ctRating ?ctRating.
 ?inf c:TapChangerInfo.ctRatio ?ctRatio.
 ?inf c:TapChangerInfo.ptRatio ?ptRatio.

}
ORDER BY ?pname ?tname ?rname ?wnum ?bus'''
    data = []
    results = gridappsd_obj.query_data(query, timeout=120)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
        datum = {thing[0]: thing[1]['value'] for thing in b.items()}
        # datum['phases'] = house_dict['phases'].split("\n")
        datum
        data.append(datum)
    return data

def get_switches(value='_C77C898B-788F-8442-5CEA-0D06ABA0693B'):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"} """

    query =prefix+'''
SELECT ?cimtype ?name ?bus1 ?bus2 ?id ?vnom1 ?trm1id ?trm2id (group_concat(distinct ?phs;separator="") as ?phases) WHERE {
  SELECT ?cimtype ?name ?bus1 ?bus2 ?phs ?id ?vnom1 ?trm1id ?trm2id WHERE {''' + fidselect + '''
 #VALUES ?fdrid {"_C77C898B-788F-8442-5CEA-0D06ABA0693B"}  # 13 bus
 VALUES ?cimraw {c:LoadBreakSwitch c:Recloser c:Breaker}
 ?fdr c:IdentifiedObject.mRID ?fdrid.
 ?s r:type ?cimraw.
  bind(strafter(str(?cimraw),"#") as ?cimtype)
 ?s c:Equipment.EquipmentContainer ?fdr.
 ?s c:IdentifiedObject.name ?name.
 ?s c:IdentifiedObject.mRID ?id.
 ?t1 c:Terminal.ConductingEquipment ?s.
 ?s c:ConductingEquipment.BaseVoltage ?lev. 
 ?lev c:BaseVoltage.nominalVoltage ?vnom1.
 ?t1 c:ACDCTerminal.sequenceNumber "1".
 ?t1 c:IdentifiedObject.mRID ?trm1id.
 ?t1 c:Terminal.ConnectivityNode ?cn1.
 ?cn1 c:IdentifiedObject.name ?bus1.
 ?t2 c:Terminal.ConductingEquipment ?s.
 ?t2 c:ACDCTerminal.sequenceNumber "2".
 ?t2 c:IdentifiedObject.mRID ?trm2id.
 ?t2 c:Terminal.ConnectivityNode ?cn2. 
 ?cn2 c:IdentifiedObject.name ?bus2
    OPTIONAL {?swp c:SwitchPhase.Switch ?s.
        ?swp c:SwitchPhase.phaseSide1 ?phsraw.
        bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs) }
 } ORDER BY ?name ?phs
}
GROUP BY ?cimtype ?name ?bus1 ?bus2 ?id ?vnom1 ?trm1id ?trm2id
ORDER BY ?cimtype ?name'''
    data = []
    # sparql.setQuery(query)
    # result = sparql.query()
    # print len(list(result.bindings))
    # for b in result.bindings:
    results = gridappsd_obj.query_data(query, timeout=120)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
        phases = b['phases']['value'].split("\n")
        datum = dict()
#         print (b['name'].value,b['bus1'].value,b['bus2'].value,b['eqid'].value,b['trm1id'].value,b['trm2id'].value, b['vnom1'].value)
#         print (b['phases'].value)
#         break
        if phases == [u'']:
            datum['phases'] = ['A', 'B', 'C']
        else:
            datum['phases'] = phases
        datum["numPhase"] = len(b['phases']['value'])
        if len(b['phases']['value']) == 0 :
            datum["numPhase"] = 3
        datum['id'] = b['id']['value']
        datum['name'] = b['name']['value']
        datum['bus1'] = b['bus1']['value']
        datum['bus2'] = b['bus2']['value']
        datum['basekv'] = b['vnom1']['value']
        datum['trm1id'] = b['trm1id']['value']
        datum['trm2id'] = b['trm2id']['value']
        data.append(datum)
    return data


def get_transformer_with_tanks(value='_40B1F0FA-8150-071D-A08F-CD104687EA5D'):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"} """
    query=prefix+'''
SELECT ?pname ?tname ?pmrid ?xfmrcode ?vgrp ?enum ?bus ?basev ?phs ?grounded ?rground ?xground ?fdrid ?trmid WHERE {
 ?p r:type c:PowerTransformer.
# feeder selection options - if all commented out, query matches all feeders
''' + fidselect + '''
#VALUES ?fdrid {"_C1C3E687-6FFD-C753-582B-632A27E28507"}  # 123 bus
#VALUES ?fdrid {"_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"}  # 13 bus
#VALUES ?fdrid {"_5B816B93-7A5F-B64C-8460-47C17D6E4B0F"}  # 13 bus assets
#VALUES ?fdrid {"_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3"}  # 8500 node
#VALUES ?fdrid {"_67AB291F-DCCD-31B7-B499-338206B9828F"}  # J1
#VALUES ?fdrid {"_9CE150A8-8CC5-A0F9-B67E-BBD8C79D3095"}  # R2 12.47 3
 ?p c:Equipment.EquipmentContainer ?fdr.
 ?fdr c:IdentifiedObject.mRID ?fdrid.
 ?p c:IdentifiedObject.name ?pname.
 ?p c:PowerTransformer.vectorGroup ?vgrp.
 ?t c:TransformerTank.PowerTransformer ?p.
 ?t c:IdentifiedObject.name ?tname.
 ?t c:IdentifiedObject.mRID ?pmrid.
 ?asset c:Asset.PowerSystemResources ?t.
 ?asset c:Asset.AssetInfo ?inf.
 ?inf c:IdentifiedObject.name ?xfmrcode.
 ?end c:TransformerTankEnd.TransformerTank ?t.
 ?end c:TransformerTankEnd.phases ?phsraw.
  bind(strafter(str(?phsraw),"PhaseCode.") as ?phs)
 ?end c:TransformerEnd.endNumber ?enum.
 ?end c:TransformerEnd.grounded ?grounded.
 OPTIONAL {?end c:TransformerEnd.rground ?rground.}
 OPTIONAL {?end c:TransformerEnd.xground ?xground.}
 ?end c:TransformerEnd.Terminal ?trm.
 ?trm c:IdentifiedObject.mRID ?trmid.
 ?trm c:Terminal.ConnectivityNode ?cn. 
 ?cn c:IdentifiedObject.name ?bus.
 ?end c:TransformerEnd.BaseVoltage ?bv.
 ?bv c:BaseVoltage.nominalVoltage ?basev
}
'''
    data = list()
    results = gridappsd_obj.query_data(query, timeout=120)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
#         if b['tname']['value'] == 'tpoletop':
#             print(b)
        # print(b['pname']['value'], b['bus']['value'], b['phs']['value'], b['basev']['value'])
        datum = {'name':  b['tname']['value'],
                 'pmrid':  b['pmrid']['value'],
                 'vgrp': b['vgrp']['value'],
                 'phases': [b['phs']['value']],
                 'bus': b['bus']['value'],
                 'basekv': b['basev']['value'],
                 'trmid':b['trmid']['value']}
        data.append(datum)
    return data


def get_transformer_no_tanks(value='_40B1F0FA-8150-071D-A08F-CD104687EA5D'):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"} """
    query=prefix+'''
# power transformers on their own; windings - DistPowerXfmrWinding
SELECT ?pname ?vgrp ?enum ?bus ?basev ?conn ?ratedS ?ratedU ?r ?ang ?grounded ?rground ?xground ?fdrid ?trmid WHERE {
''' + fidselect + '''
# feeder selection options - if all commented out, query matches all feeders
#VALUES ?fdrid {"_C1C3E687-6FFD-C753-582B-632A27E28507"}  # 123 bus
#VALUES ?fdrid {"_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"}  # 13 bus
#VALUES ?fdrid {"_5B816B93-7A5F-B64C-8460-47C17D6E4B0F"}  # 13 bus assets
#VALUES ?fdrid {"_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3"}  # 8500 node
#VALUES ?fdrid {"_67AB291F-DCCD-31B7-B499-338206B9828F"}  # J1
#VALUES ?fdrid {"_9CE150A8-8CC5-A0F9-B67E-BBD8C79D3095"}  # R2 12.47 3
 ?p c:Equipment.EquipmentContainer ?fdr.
 ?fdr c:IdentifiedObject.mRID ?fdrid.
 ?p r:type c:PowerTransformer.
 ?p c:IdentifiedObject.name ?pname.
 ?p c:PowerTransformer.vectorGroup ?vgrp.
 ?end c:PowerTransformerEnd.PowerTransformer ?p.
 ?end c:TransformerEnd.endNumber ?enum.
 ?end c:PowerTransformerEnd.ratedS ?ratedS.
 ?end c:PowerTransformerEnd.ratedU ?ratedU.
 ?end c:PowerTransformerEnd.r ?r.
 ?end c:PowerTransformerEnd.phaseAngleClock ?ang.
 ?end c:PowerTransformerEnd.connectionKind ?connraw.  
  bind(strafter(str(?connraw),"WindingConnection.") as ?conn)
 ?end c:TransformerEnd.grounded ?grounded.
 OPTIONAL {?end c:TransformerEnd.rground ?rground.}
 OPTIONAL {?end c:TransformerEnd.xground ?xground.}
 ?end c:TransformerEnd.Terminal ?trm.
 ?trm c:IdentifiedObject.mRID ?trmid.
 ?trm c:Terminal.ConnectivityNode ?cn. 
 ?cn c:IdentifiedObject.name ?bus.
 ?end c:TransformerEnd.BaseVoltage ?bv.
 ?bv c:BaseVoltage.nominalVoltage ?basev
}
ORDER BY ?pname ?enum'''
    data = list()
    # TODO check : I think these are all 3 phases?
    results = gridappsd_obj.query_data(query, timeout=120)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
#         print ( b['pname']['value'], b['bus']['value'],  b['basev']['value'] )
        datum = {'name':  b['pname']['value'],
                 'basekv': b['basev']['value'],
                 'bus':b['bus']['value'],
                 'trmid': b['trmid']['value']}
        datum['phases'] = ['A', 'B', 'C']
        if 'rground' in b and float(b['xground']['value']) != 0:
            datum['phases'] = ['A', 'B', 'C', 'N']
        data.append(datum)
    return data


def get_basev_from(lines, switches, trans1, trans2):
    data = {}
    for line in lines+switches:
        for phase in line['phases']:
            # print(line['bus1'].upper() + ' ' + phase)
            # print(line['bus2'].upper() + ' ' + phase)
            data[line['bus1'].upper() + '.' + lookup[phase]] = float(line['basekv']) / math.sqrt(3)
            data[line['bus2'].upper() + '.' + lookup[phase]] = float(line['basekv']) / math.sqrt(3)

    for trans in trans1:
        for phase in trans['phases']:
            # print( trans['bus'].upper() + ' ' + phase)
            data[trans['bus'].upper() + '.' + lookup[phase]] = float(trans['basekv']) / math.sqrt(3)
    for trans in trans2:
        for phase in trans['phases']:
            # print(trans['bus'].upper() + ' ' + phase)
            data[trans['bus'].upper() + '.' + lookup[phase]] = float(trans['basekv']) / math.sqrt(3)

    return data


def get_trmid_from(lines, trans1, trans2):
    data = {}
    for line in lines:
        for phase in line['phases']:
            #         print line
            data[line['bus1'].upper() + '.' + lookup[phase]] = line['trm1id']
            data[line['bus2'].upper() + '.' + lookup[phase]] = line['trm2id']

    for trans in trans1:
        for phase in trans['phases']:
            data[trans['bus'].upper() + '.' + lookup[phase]] = trans['trmid']
    for trans in trans2:
        for phase in trans['phases']:
            data[trans['bus'].upper() + '.' + lookup[phase]] = trans['trmid']

    return data


def lookup_meas(feeder =u'_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3'):
    # list all measurements, with buses and equipments - DistMeasurement
    query = prefix+'''
    SELECT ?class ?type ?name ?bus ?phases ?eqtype ?eqname ?eqid ?trmid ?id ?ce WHERE {
     VALUES ?fdrid {"'''+feeder+'''"}
     ?eq c:Equipment.EquipmentContainer ?fdr.
     ?fdr c:IdentifiedObject.mRID ?fdrid. 
    { ?s r:type c:Discrete. bind ("Discrete" as ?class)}
      UNION
    { ?s r:type c:Analog. bind ("Analog" as ?class)}
     ?s c:IdentifiedObject.name ?name .
     ?s c:IdentifiedObject.mRID ?id .
     ?s c:Measurement.PowerSystemResource ?eq .
     ?s c:Measurement.Terminal ?trm .
     ?trm c:Terminal.ConductingEquipment ?ce.
     ?s c:Measurement.measurementType ?type .
     ?trm c:IdentifiedObject.mRID ?trmid.
     ?eq c:IdentifiedObject.mRID ?eqid.
     ?eq c:IdentifiedObject.name ?eqname.
     ?eq r:type ?typeraw.
      bind(strafter(str(?typeraw),"#") as ?eqtype)
     ?trm c:Terminal.ConnectivityNode ?cn.
     ?cn c:IdentifiedObject.name ?bus.
     ?s c:Measurement.phases ?phsraw .
       {bind(strafter(str(?phsraw),"PhaseCode.") as ?phases)}
    } ORDER BY ?class ?type ?name
    '''
    result = {}
    name_map = {}
    node_name_map_va_power = {}
    node_name_map_pnv_voltage = {}
    pec_map = {}
    load_power_map = {}
    load_voltage_map = {}
    switch_pos = {}
    cap_pos = {}
    tap_pos = {}
    line_map = {}
    line_voltage_map = {}
    trans_map = {}
    trans_voltage_map = {}
    results = gridappsd_obj.query_data(query)
    results_obj = results['data']
    mrid_types = set()
    for b in results_obj['results']['bindings']:
        # print (b['bus']['value'], b['phases']['value'], b['eqtype']['value'], b['eqname']['value'], b['eqid']['value'], b['trmid']['value'] )
        result[b['id']['value']] = {'name': b['name']['value'], 'type':b['type']['value'], 'phases': b['phases']['value'],
                                   'trmid': b['trmid']['value'], 'bus': b['bus']['value'], 'ce': b['ce']['value'], 'eqid': b['eqid']['value']}
        # if '_FC92A30E-1CFD-FB31-7705-36D3480C8395'  == b['trmid']['value']:
        #     print('found _FC92A30E-1CFD-FB31-7705-36D3480C8395' + b['id']['value'] + ' '+ b['type']['value'])

#         name_map[b['id']['value']] = b['name']['value']
#         print(b['eqtype']['value'] )
        name_map[b['trmid']['value']] = b['id']['value']
        # print(b['name']['value'], b['id']['value'],b['eqtype']['value'], b['bus']['value'].upper())
        # print(b['phases']['value'])
        if b['phases']['value'] != 'none': # Soc
            name = b['bus']['value'].upper() + '.' + lookup[b['phases']['value']]
        mrid_types.add(b['eqtype']['value'])
        # print(name, b['type']['value'], b['eqtype']['value'],  b['eqtype']['value'] == u'EnergyConsumer')
        if b['type']['value'] == 'Pos':
            if b['eqtype']['value'] == u'LoadBreakSwitch':
                switch_pos[name] = b['id']['value']
            if b['eqtype']['value'] == u'LinearShuntCompensator':
                cap_pos[name] = b['id']['value']
            if b['eqtype']['value'] == u'PowerTransformer':
                tap_pos[name] = b['id']['value']
        if b['type']['value'] == 'VA':
            node_name_map_va_power[name] = b['id']['value']
            if b['eqtype']['value'] == u'PowerElectronicsConnection':
                pec_map[name] = b['id']['value']
            if b['eqtype']['value'] == u'EnergyConsumer':
                load_power_map[name] = b['id']['value']
            if b['eqtype']['value'] == u'PowerTransformer':
                trans_map[name] = b['id']['value']
            if b['eqtype']['value'] == u'ACLineSegment' :
                if name in line_map:
                    line_map[name].append(b['id']['value'])
                else:
                    line_map[name] = [b['id']['value']]
        if b['type']['value'] == 'PNV':
            node_name_map_pnv_voltage[name] = b['id']['value']
            if b['phases']['value'].lower() == "s1" and b['eqtype']['value'] == u'ACLineSegment':
                node_name_map_pnv_voltage[b['bus']['value'].upper() + '.2'] = \
                    b['id']['value']
            # HACK FOR SPLIT PHASE

            if b['eqtype']['value'] == u'EnergyConsumer':
                load_voltage_map[name] = b['id']['value']
            if b['eqtype']['value'] == u'PowerTransformer':
                trans_voltage_map[name] = b['id']['value']
            if b['eqtype']['value'] == u'ACLineSegment':
                if name in line_voltage_map:
                    line_voltage_map[name].append(b['id']['value'])
                else:
                    line_voltage_map[name] = [b['id']['value']]

            # if b['eqtype']['value'] == u'PowerElectronicsConnection' :
            #     pec_map[name] = b['id']['value']
    # print(mrid_types)
    # print(trans_map)
    return result, name_map, node_name_map_va_power, node_name_map_pnv_voltage, pec_map, load_power_map, line_map, \
           trans_map, switch_pos, cap_pos, tap_pos, load_voltage_map, line_voltage_map, trans_voltage_map


#Remap list to measid : values
def get_meas_map(message):
    '''
    Convert measurements to map
    '''
    # meas_map = {}
    # if 'message' in message:
    #     for meas in message['message']['measurements']:
    #     #     print meas
    #         if u'magnitude' in meas:
    #             meas_map[meas['measurement_mrid']] = {u'magnitude':meas[u'magnitude'], u'angle':meas[u'angle']}
    if 'message' in message:
        meas_map = {meas['measurement_mrid']:meas for meas in message['message']['measurements']}
        return meas_map
    return {}


## Map names to measurements in the y matrix order
def get_YVANode(nodenames, nodename_term_map, meas_map, name_map):
    YVANode = list()
    for nn in nodenames:
        if nn not in nodename_term_map:
            print(nn)
        else:
            if nodename_term_map[nn] not in name_map:
                print('No id for node '+nn +' terminal mrid:'+ nodename_term_map[nn])
                YVANode.append(complex(0,0))
            else:
                mid = name_map[nodename_term_map[nn]]
    #             print 'Meas id ' + mid
                if mid in meas_map:
                    print(meas_map[mid])
                    YVANode.append(complex(meas_map[mid]['magnitude'],  meas_map[mid]['angle']))
                else:
                    YVANode.append(complex(0,0))
    return YVANode

def get_pos(meas_map, node_name_map):
    '''
    Get the Position from the node map and measurment map
    :param nodenames:
    :param meas_map:
    :param node_name_map:
    :param convert_to_radian: # Convert degrees to radian
    :return:
    '''
    pos = {}
    for name, v in node_name_map.items():
        if v in meas_map:
            meas = meas_map[v]
            pos[name] = meas['value']
        else:
            pos[name] = None
    return pos

def get_PQNode(nodenames, meas_map, node_name_map, convert_to_radian=False):
    '''
    Get the P and Q from the node map and measurment map
    :param nodenames:
    :param meas_map:
    :param node_name_map:
    :param convert_to_radian: # Convert degrees to radian
    :return:
    '''
    PQNode = []
    for nn in nodenames:
        if nn not in node_name_map:
            # print('No node named  ' +nn)
            PQNode.append(complex(0,0))
        else:
            # complex(grid_v_test.real * math.cos(grid_v_test.imag / 180 * 3.14), grid_v_test.real * math.sin(grid_v_test.imag / 180 * 3.14))

            mid = node_name_map[nn]
            if mid in meas_map:
                # PQNode.append(complex(meas_map[mid]['magnitude'], meas_map[mid]['angle']))
                temp = complex(meas_map[mid]['magnitude'], meas_map[mid]['angle'])
                if convert_to_radian:
                    # temp = complex(*degrees2rad(temp))
                    temp = complex(*pol2cart(meas_map[mid]['magnitude'], meas_map[mid]['angle']))
                PQNode.append(temp)
            else:
                print('No PQ meas for ' + nn + ' ' + mid)
                PQNode.append(complex(0,0))
    return PQNode

def pol2cart(rho, phi):
    phi=np.radians(phi)
    x = rho * np.cos(phi)
    y = rho * np.sin(phi)
    return(x, y)

def cart2pol(x, y):
    rho = np.sqrt(x**2 + y**2)
    phi = np.arctan2(y, x)
    return(rho, np.degrees(phi))

def polar2rect(magnitude, angle):
    # return temp.real*math.cos(temp.imag * (math.pi / 180)), temp.real*math.sin(temp.imag * (math.pi / 180))
    res = cmath.rect(magnitude, math.radians(angle))
    return res.real, res.imag

def rect2polar(temp):
    # real = math.sqrt(temp.real*temp.real + temp.imag*temp.imag)
    # imag = math.atan2(temp.imag,temp.real) * 180 / math.pi
    # return real,imag
    res = cmath.polar(temp)
    return res[0], math.degrees(res[1])

def degrees2rad(temp):
    res = complex(temp.real * math.cos(temp.imag / 180 * 3.14), temp.real * math.sin(temp.imag / 180 * 3.14))
    return res.real, res.imag

def rad2degrees(temp):
    res = complex(abs(temp),math.degrees(math.atan2(temp.imag, temp.real)))
    return res.real, res.imag


def get_cap_PQ(caps, AllNodeNames, meas_map, node_name_map):
    Qcap = [0] * len(AllNodeNames)
    # print name_map
    for cap in caps:
        for ii in range(cap["numPhase"]):
            node_name = cap["busname"].upper()+'.'+cap["busphase"][ii]
            index = AllNodeNames.index(node_name)
            # print 'Cap index ' + node_name + ' ' + str(index)
            # Qcap[index] = -cap["power"][2*ii-1]
            Qcap[index] = 0.0
            if node_name_map[node_name] in meas_map:
                Qcap[index] = - meas_map[node_name_map[node_name]]['angle'] # is angle right?
    return Qcap


def get_pv_PQ(datum_dict, AllNodeNames, meas_map, node_name_map, convert_to_radian=False):
    P_values = [0] * len(AllNodeNames)
    Q_values = [0] * len(AllNodeNames)
    mrid = []
    # print name_map
    for data in datum_dict.items():
        for ii in range(data[1]["numPhase"]):
            # node_name = data[1]["busname"].upper()+'.'+data[1]["busphase"][ii]
            node_name = data[1]["busname"]
            index = AllNodeNames.index(node_name)
            # print('Name index ' + node_name + ' ' + str(index) )
            P_values[index] = 0.0
            Q_values[index] = 0.0
            if node_name_map[node_name] in meas_map:
                # print (meas_map[node_name_map[node_name]])
                mrid.append(node_name_map[node_name])

                magnitude = meas_map[node_name_map[node_name]]['magnitude']
                angle = meas_map[node_name_map[node_name]]['angle']
                data[1]['polar']['p'] = magnitude
                data[1]['polar']['q'] = angle
                if convert_to_radian:
                    # magnitude, angle = degrees2rad(complex(magnitude, angle))
                    magnitude, angle = pol2cart(magnitude, angle)
                    magnitude = magnitude
                    angle = angle
                P_values[index] = magnitude  # is angle right?
                Q_values[index] = angle  # is angle right?
                data[1]['current_time']['p'] = magnitude
                data[1]['current_time']['q'] = angle
            else:
                print("No value for " + node_name + " " + str(node_name_map[node_name]))
                data[1]['current_time']['p'] = 0.0
                data[1]['current_time']['q'] = 0.0
                data[1]['polar']['p'] = 0.0
                data[1]['polar']['q'] = 0.0

    return P_values, Q_values, mrid


def get_capacitors(value='_67AB291F-DCCD-31B7-B499-338206B9828F'):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"} """

    query = prefix + '''
    # capacitors (does not account for 2+ unequal phases on same LinearShuntCompensator) - DistCapacitor
SELECT ?name ?basev ?nomu ?bsection ?bus ?conn ?grnd ?phs ?ctrlenabled ?discrete ?mode ?deadband ?setpoint ?delay ?monclass ?moneq ?monbus ?monphs ?id ?fdrid ?tmrid WHERE {
 ?s r:type c:LinearShuntCompensator.
# feeder selection options - if all commented out, query matches all feeders
 ''' + fidselect + '''
 ?s c:Equipment.EquipmentContainer ?fdr.
 ?fdr c:IdentifiedObject.mRID ?fdrid.
 ?s c:IdentifiedObject.name ?name.
 ?s c:ConductingEquipment.BaseVoltage ?bv.
 ?bv c:BaseVoltage.nominalVoltage ?basev.
 ?s c:ShuntCompensator.nomU ?nomu. 
 ?s c:LinearShuntCompensator.bPerSection ?bsection. 
 ?s c:ShuntCompensator.phaseConnection ?connraw.
   bind(strafter(str(?connraw),"PhaseShuntConnectionKind.") as ?conn)
 ?s c:ShuntCompensator.grounded ?grnd.
 OPTIONAL {?scp c:ShuntCompensatorPhase.ShuntCompensator ?s.
 ?scp c:ShuntCompensatorPhase.phase ?phsraw.
   bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs) }
 OPTIONAL {?ctl c:RegulatingControl.RegulatingCondEq ?s.
          ?ctl c:RegulatingControl.discrete ?discrete.
          ?ctl c:RegulatingControl.enabled ?ctrlenabled.
          ?ctl c:RegulatingControl.mode ?moderaw.
           bind(strafter(str(?moderaw),"RegulatingControlModeKind.") as ?mode)
          ?ctl c:RegulatingControl.monitoredPhase ?monraw.
           bind(strafter(str(?monraw),"PhaseCode.") as ?monphs)
          ?ctl c:RegulatingControl.targetDeadband ?deadband.
          ?ctl c:RegulatingControl.targetValue ?setpoint.
          ?s c:ShuntCompensator.aVRDelay ?delay.
          ?ctl c:RegulatingControl.Terminal ?trm.
          ?trm c:Terminal.ConductingEquipment ?eq.
          ?eq a ?classraw.
           bind(strafter(str(?classraw),"cim17#") as ?monclass)
          ?eq c:IdentifiedObject.name ?moneq.
          ?trm c:Terminal.ConnectivityNode ?moncn.
          ?moncn c:IdentifiedObject.name ?monbus.
          }
 ?s c:IdentifiedObject.mRID ?id. 
 ?t c:Terminal.ConductingEquipment ?s.
 ?t c:IdentifiedObject.mRID ?tmrid. 
 ?t c:Terminal.ConnectivityNode ?cn. 
 ?cn c:IdentifiedObject.name ?bus
}
ORDER by ?name'''
    data = []
    results = gridappsd_obj.query_data(query)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
        # print (b['name']['value'], b['basev']['value'], b['nomu']['value'], b['bsection']['value'], b['bus']['value'], b['conn']['value'],
        #        b['grnd']['value'], b['id']['value'])

        datum = dict()
        if 'phs' not in b:
            # datum['busphase'] = ['A', 'B', 'C']
            datum['busphase'] = ['1', '2', '3']
        else:
            phases = b['phs']['value'].split(',')
            datum['busphase'] = [lookup[phs] for phs in phases]

        datum["name"] = b['name']['value']
        temp = b['bus']['value'].split('.')
        datum["busname"] = b['bus']['value']
        datum["numPhase"] = len(datum['busphase'])
        datum["kVar"] = b['nomu']['value']
        # datum["power"] = b['nomu']['value']  # dss.CktElement.Powers()[0:2 * NumPhase]
        datum["power"] = [0.0 for i in range(datum['numPhase'] * 2)]
        #  datum['power'] = [float(b['p']['value']) * vmult, float(b['q']['value']) * vmult] is this from bpersection?
        numSteps = 1 # ?
        cap_b = numSteps * float(b['bsection']['value'])
        cap_v = float(b['nomu']['value']) * vmult
        datum["mrid"] = b['id']['value']
        datum["tmrid"] = b['tmrid']['value']
        datum["kv"] = cap_v
        datum["kVar"] = cap_v * cap_v * cap_b * 1000.0
        data.append(datum)
    return data

def get_loads_query(value='_67AB291F-DCCD-31B7-B499-338206B9828F',load_scale=1.):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"} """

    query = prefix+'''
    SELECT ?name ?bus ?basev ?p ?q ?conn ?pz ?qz ?pi ?qi ?pp ?qp ?pe ?qe ?fdrid (group_concat(distinct ?phs;separator="\\n") as ?phases) 
    WHERE {
    ?s r:type c:EnergyConsumer.
    ''' + fidselect + '''
     # feeder selection options - if all commented out, query matches all feeders
    ?s c:Equipment.EquipmentContainer ?fdr.
     ?fdr c:IdentifiedObject.mRID ?fdrid.
     ?s c:IdentifiedObject.name ?name.
     ?s c:ConductingEquipment.BaseVoltage ?bv.
     ?bv c:BaseVoltage.nominalVoltage ?basev.
     ?s c:EnergyConsumer.p ?p.
     ?s c:EnergyConsumer.q ?q.
     ?s c:EnergyConsumer.phaseConnection ?connraw.
     bind(strafter(str(?connraw),"PhaseShuntConnectionKind.") as ?conn)
       ?s c:EnergyConsumer.LoadResponse ?lr.
     ?lr c:LoadResponseCharacteristic.pConstantImpedance ?pz.
     ?lr c:LoadResponseCharacteristic.qConstantImpedance ?qz.
     ?lr c:LoadResponseCharacteristic.pConstantCurrent ?pi.
     ?lr c:LoadResponseCharacteristic.qConstantCurrent ?qi.
     ?lr c:LoadResponseCharacteristic.pConstantPower ?pp.
     ?lr c:LoadResponseCharacteristic.qConstantPower ?qp.
     ?lr c:LoadResponseCharacteristic.pVoltageExponent ?pe.
     ?lr c:LoadResponseCharacteristic.qVoltageExponent ?qe.
     OPTIONAL {?ecp c:EnergyConsumerPhase.EnergyConsumer ?s.
     ?ecp c:EnergyConsumerPhase.phase ?phsraw.
     bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs) }
       ?t c:Terminal.ConductingEquipment ?s.
     ?t c:Terminal.ConnectivityNode ?cn.
     ?cn c:IdentifiedObject.name ?bus
     }
    GROUP BY ?name ?bus ?basev ?p ?q ?conn ?pz ?qz ?pi ?qi ?pp ?qp ?pe ?qe ?fdrid
    ORDER by ?name
    '''

    data = []
    total_load = 0
    results = gridappsd_obj.query_data(query)
    results_obj = results['data']
    for b in results_obj['results']['bindings']:
        phases = b['phases']['value'].split(',')
        datum = dict()
        datum["name"] = b['name']['value']
        datum["kV"] = vmult * float(b['basev']['value']) / math.sqrt(3.0)  # if ((phs_cnt < 2) && phs_conn.contains("w"))
        datum["kW"] = float(b['p']['value']) / 1000.
        datum["kVar"] = float(b['q']['value']) / 1000.
        datum["PF"] = 1.  # ?
        datum["Delta_conn"] = b['conn']['value'] != 'Y'
        datum['bus'] = b['bus']['value']
        if '\n' in phases[0]:
            phases = phases[0].split('\n')
        if phases == [u'']:
            datum['phases'] = ['A', 'B', 'C']
        else:
            datum['phases'] = phases
        if phases == [u'']:
            datum['busphase'] = ['1', '2', '3']
        else:
            datum['busphase'] = [lookup[phs] for phs in phases]

        datum['numPhase'] = len(datum['busphase'])
        datum['power'] = [float(b['pp']['value']) * vmult, float(b['qp']['value']) * vmult]
        datum['pfixed'] = float(b['p']['value']) * vmult
        datum['qfixed'] = float(b['q']['value']) * vmult
        constant_currents = {}
        for phase in datum['phases']:
            if 's' in phase:
                # print('I dont know how to do secondary yet')
                constant_currents[phase] = str(0j)
            else:
                temp_current = ((complex(datum['kW'], datum['kVar']) *load_scale)/
                                (datum['kV'] * datum['numPhase'] * phase_shift[phase])).conjugate()
                constant_currents[phase] = str(temp_current)
        datum["constant_currents"] = constant_currents
        # print (datum)

        data.append(datum)
        total_load += datum["kW"]

    return data, total_load


def get_source_node_names(fidselect):
    sources = get_source(fidselect)
    source_node_names = []
    for source in sources:
        for index in range(source["numPhase"]):
            node_name = source["busname"].upper() + '.' + source["busphase"][index]
            source_node_names.append(node_name)
    return source_node_names


if __name__ == '__main__':
    # fid_select = '_67AB291F-DCCD-31B7-B499-338206B9828F'
    fid_select_123 = '_C1C3E687-6FFD-C753-582B-632A27E28507'
    fid_select = '_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D' # Mine 123pv'
    # fid_select = '_C77C898B-788F-8442-5CEA-0D06ABA0693B'
    # fid_select = '_EBDB5A4A-543C-9025-243E-8CAD24307380'
    # fid_select = '_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3'
    # fid_select = '_C1C3E687-6FFD-C753-582B-632A27E28507'
    fid_select = '_AAE94E4A-2465-6F5E-37B1-3E72183A4E44'
    # fid_select = '_DA00D94F-4683-FD19-15D9-8FF002220115'  # mine with house
    fid_select = '_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62'
    fid_select = '_C1C3E687-6FFD-C753-582B-632A27E28507'
    fid_select = '_AAE94E4A-2465-6F5E-37B1-3E72183A4E44'


    # loads = get_loads_query(fid_select)
    # print(loads)
#'_e1307d5d-ce0b-44bb-82f4-30ef9a193c15'
    sources = get_source(fid_select)
    print(sources)
    result, name_map, node_name_map_va_power, node_name_map_pnv_voltage, pec_map, load_power_map, line_map, \
    trans_map, switch_pos, cap_pos, tap_pos, load_voltage_map, line_voltage_map, trans_voltage_map = lookup_meas(fid_select)
    _source_node_names = get_source_node_names(fid_select)
    for node_name in _source_node_names:
        if node_name in line_map:
            term = line_map[node_name][0]
            # print(node_name, term)
            # source_total += complex(meas_map[term][u'magnitude'], meas_map[term][u'angle'])
        elif node_name in trans_map:
            term = trans_map[node_name]
            print(node_name, term)
            # source_total += complex(meas_map[term][u'magnitude'], meas_map[term][u'angle'])
        else:
            print("Figure out how to do this with transformer ends.")
    # exit(0)

    # pvs = get_solar(fid_select)
    # print(pvs)
    # exit(0)
    # lookup_meas('_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3')
    source = get_source(fid_select)
    print(source)

    _source_node_names = get_source_node_names(fid_select)
    print(_source_node_names)
    # get_loads_query('_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62')
    # caps = get_capacitors(fid_select_123)
    caps = get_capacitors(fid_select)
    print(caps)

    regs = get_regulator(fid_select)
    print(regs)

    switches = get_switches(fid_select)
    # self._switches = query_model.get_switches(self.fidselect)
    # self._switch_name_map = {switch['bus1'].upper() + '.' + query_model.lookup[switch['phases'][0]]: switch['name'] for
    #                          switch in self._switches}
    print(switches)

    lines = get_line_segements(fid_select)
    # print(lines)
    # exit(0)

    # pvs = get_solar('_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62')
    # print(pvs)
    pvs = get_solar(fid_select)
    print(pvs)
    exit(0)

    loads = get_loads_query(fid_select)

    # get_feeder('ieee123')
    # print(get_feeder_name(''))

    print(get_source_node_names(fid_select))

    # switches = get_switches(fid_select)
    # lines = get_line_segements(fid_select)
    # print(lines)
    # exit(0)

    trans1 = get_transformer_with_tanks(fid_select)
    trans2 = get_transformer_no_tanks(fid_select)
    test_data = get_basev_from(lines, switches, trans1, trans2)
    print(trans1)
    print(trans2)
    print(test_data)
    # exit(0)
    nodename_term_map = get_trmid_from(lines, trans1, trans2)
    # result, name_map, node_name_map, node_name_map_pnv = lookup_meas('', fidselect)
    result, name_map, node_name_map_va_power, node_name_map_pnv_voltage, pec_map, load_power_map, line_map, \
           trans_map, cap_pos, tap_pos, load_voltage_map, line_voltage_map, trans_voltage_map = lookup_meas(fid_select)
    print(pec_map)
    print('##### TRANS #####')
    print(trans_map)
    print(tap_pos)
    print(load_voltage_map)

    exit(0)
    node_names_123 = ['150.1', '150.2', '150.3', '150R.1', '150R.2', '150R.3', '149.1', '149.2', '149.3', '1.1', '1.2',
                      '1.3', '2.2', '3.3', '7.1', '7.2', '7.3', '4.3', '5.3', '6.3', '8.1', '8.2', '8.3', '12.2', '9.1',
                      '13.1', '13.2', '13.3', '9R.1', '14.1', '34.3', '18.1', '18.2', '18.3', '11.1', '10.1', '15.3',
                      '16.3', '17.3', '19.1', '21.1', '21.2', '21.3', '20.1', '22.2', '23.1', '23.2', '23.3', '24.3',
                      '25.1', '25.2', '25.3', '25R.1', '25R.3', '26.1', '26.3', '28.1', '28.2', '28.3', '27.1', '27.3',
                      '31.3', '33.1', '29.1', '29.2', '29.3', '30.1', '30.2', '30.3', '250.1', '250.2', '250.3', '32.3',
                      '35.1', '35.2', '36.1', '36.2', '35.3', '40.1', '40.2', '40.3', '37.1', '38.2', '39.2', '41.3',
                      '42.1', '42.2', '42.3', '43.2', '44.1', '44.2', '44.3', '45.1', '47.1', '47.2', '47.3', '46.1',
                      '48.1', '48.2', '48.3', '49.1', '49.2', '49.3', '50.1', '50.2', '50.3', '51.1', '51.2', '51.3',
                      '151.1', '151.2', '151.3', '52.1', '52.2', '52.3', '53.1', '53.2', '53.3', '54.1', '54.2', '54.3',
                      '55.1', '55.2', '55.3', '57.1', '57.2', '57.3', '56.1', '56.2', '56.3', '58.2', '60.1', '60.2',
                      '60.3', '59.2', '61.1', '61.2', '61.3', '62.1', '62.2', '62.3', '63.1', '63.2', '63.3', '64.1',
                      '64.2', '64.3', '65.1', '65.2', '65.3', '66.1', '66.2', '66.3', '67.1', '68.1', '67.2', '67.3',
                      '72.1', '72.2', '72.3', '97.1', '97.2', '97.3', '69.1', '70.1', '71.1', '73.3', '76.1', '76.2',
                      '76.3', '74.3', '75.3', '77.1', '77.2', '77.3', '86.1', '86.2', '86.3', '78.1', '78.2', '78.3',
                      '79.1', '79.2', '79.3', '80.1', '80.2', '80.3', '81.1', '81.2', '81.3', '82.1', '82.2', '82.3',
                      '84.3', '83.1', '83.2', '83.3', '85.3', '87.1', '87.2', '87.3', '88.1', '89.1', '89.2', '89.3',
                      '90.2', '91.1', '91.2', '91.3', '92.3', '93.1', '93.2', '93.3', '94.1', '95.1', '95.2', '95.3',
                      '96.2', '98.1', '98.2', '98.3', '99.1', '99.2', '99.3', '100.1', '100.2', '100.3', '450.1',
                      '450.2', '450.3', '197.1', '197.2', '197.3', '101.1', '101.2', '101.3', '102.3', '105.1', '105.2',
                      '105.3', '103.3', '104.3', '106.2', '108.1', '108.2', '108.3', '107.2', '109.1', '300.1', '300.2',
                      '300.3', '110.1', '111.1', '112.1', '113.1', '114.1', '135.1', '135.2', '135.3', '152.1', '152.2',
                      '152.3', '160R.1', '160R.2', '160R.3', '160.1', '160.2', '160.3', '61S.1', '61S.2', '61S.3',
                      '300_OPEN.1', '300_OPEN.2', '300_OPEN.3', '94_OPEN.1', '610.1', '610.2', '610.3']

    node_names = ['149.1', '149.2', '149.3', '1.1', '1.2', '1.3', '2.2', '3.3', '7.1', '7.2', '7.3', '4.3', '5.3',
                  '6.3', '8.1', '8.2', '8.3', '12.2', '9.1', '13.1', '13.2', '13.3', '14.1', '34.3', '18.1', '18.2',
                  '18.3', '11.1', '10.1', '15.3', '16.3', '17.3', '19.1', '21.1', '21.2', '21.3', '20.1', '22.2',
                  '23.1', '23.2', '23.3', '24.3', '25.1', '25.2', '25.3', '26.1', '26.3', '28.1', '28.2', '28.3',
                  '27.1', '27.3', '31.3', '33.1', '29.1', '29.2', '29.3', '30.1', '30.2', '30.3', '250.1', '250.2',
                  '250.3', '32.3', '35.1', '35.2', '36.1', '36.2', '35.3', '40.1', '40.2', '40.3', '37.1', '38.2',
                  '39.2', '41.3', '42.1', '42.2', '42.3', '43.2', '44.1', '44.2', '44.3', '45.1', '47.1', '47.2',
                  '47.3', '46.1', '48.1', '48.2', '48.3', '49.1', '49.2', '49.3', '50.1', '50.2', '50.3', '51.1',
                  '51.2', '51.3', '151.1', '151.2', '151.3', '52.1', '52.2', '52.3', '53.1', '53.2', '53.3', '54.1',
                  '54.2', '54.3', '55.1', '55.2', '55.3', '57.1', '57.2', '57.3', '56.1', '56.2', '56.3', '58.2',
                  '60.1', '60.2', '60.3', '59.2', '61.1', '61.2', '61.3', '62.1', '62.2', '62.3', '63.1', '63.2',
                  '63.3', '64.1', '64.2', '64.3', '65.1', '65.2', '65.3', '66.1', '66.2', '66.3', '67.1', '68.1',
                  '67.2', '67.3', '72.1', '72.2', '72.3', '97.1', '97.2', '97.3', '69.1', '70.1', '71.1', '73.3',
                  '76.1', '76.2', '76.3', '74.3', '75.3', '77.1', '77.2', '77.3', '86.1', '86.2', '86.3', '78.1',
                  '78.2', '78.3', '79.1', '79.2', '79.3', '80.1', '80.2', '80.3', '81.1', '81.2', '81.3', '82.1',
                  '82.2', '82.3', '84.3', '83.1', '83.2', '83.3', '85.3', '87.1', '87.2', '87.3', '88.1', '89.1',
                  '89.2', '89.3', '90.2', '91.1', '91.2', '91.3', '92.3', '93.1', '93.2', '93.3', '94.1', '95.1',
                  '95.2', '95.3', '96.2', '98.1', '98.2', '98.3', '99.1', '99.2', '99.3', '100.1', '100.2', '100.3',
                  '450.1', '450.2', '450.3', '197.1', '197.2', '197.3', '101.1', '101.2', '101.3', '102.3', '105.1',
                  '105.2', '105.3', '103.3', '104.3', '106.2', '108.1', '108.2', '108.3', '107.2', '109.1', '300.1',
                  '300.2', '300.3', '110.1', '111.1', '112.1', '113.1', '114.1', '135.1', '135.2', '135.3', '152.1',
                  '152.2', '152.3', '160.1', '160.2', '160.3', '61S.1', '61S.2', '61S.3', '300_OPEN.1', '300_OPEN.2',
                  '300_OPEN.3', '94_OPEN.1']

    sample_input = {u'message': {u'measurements': [{u'angle': -37.65944666586626,
                                                    u'magnitude': 51500.29844588713,
                                                    u'measurement_mrid': u'_2cb00c51-a83a-4079-8c2d-945aa71f7abe'},
                                                   {u'angle': 21.152301019110592,
                                                    u'magnitude': 45409.388608630135,
                                                    u'measurement_mrid': u'_92e034f1-623f-4f2f-8731-05b03b308294'},
                                                   {u'angle': -31.047632770014765,
                                                    u'magnitude': 22772.18492152509,
                                                    u'measurement_mrid': u'_a1714ad4-6bf4-4131-89e0-6e2217c58ad0'},
                                                   {u'angle': -37.03029741817235,
                                                    u'magnitude': 50285.18027056873,
                                                    u'measurement_mrid': u'_b848d346-d303-4d7d-8a03-637bf949f193'},
                                                   {u'angle': 23.525774791657707,
                                                    u'magnitude': 65841.3302251245,
                                                    u'measurement_mrid': u'_9f81ef63-f0f0-4fd4-a7b9-f8e124f4ad0b'},
                                                   {u'angle': -83.68870915446604,
                                                    u'magnitude': 185314.2868441449,
                                                    u'measurement_mrid': u'_0cbb4088-3251-4685-9880-e5d925ec8113'},
                                                   {u'angle': -77.38898319796519,
                                                    u'magnitude': 196295.2232402142,
                                                    u'measurement_mrid': u'_3361ce47-ef08-4827-bf2f-5f90582df734'},
                                                   {u'angle': -89.94754536304852,
                                                    u'magnitude': 206408.48564878086,
                                                    u'measurement_mrid': u'_42c4749e-25c6-4da9-a86d-bf1e654a8cd8'}
                                                   ],
                                 u'timestamp': 1535734232},
                    u'simulation_id': u'1352961375'}

    meas_map = get_meas_map(sample_input)
    YVANode = get_YVANode(node_names_123, nodename_term_map, meas_map, name_map)
    # print (node_meas['1.1'])

    # Look up node meas by using line mrid
    # get all the lines and get bus1 name

    test_data, total_load = get_loads_query()
    print(len(test_data))
    print(total_load)

    get_pv_PQ(pvs, node_names, meas_map, pec_map)
