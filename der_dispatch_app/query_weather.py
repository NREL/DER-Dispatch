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
import sys
import pandas as pd
sys.path.append('/usr/src/gridappsd-python')
from gridappsd import GridAPPSD


__GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
print(__GRIDAPPSD_URI__)
if __GRIDAPPSD_URI__ == 'localhost:61613':
    gridappsd_obj = GridAPPSD(1234)
else:
    from gridappsd import utils
    # gridappsd_obj = GridAPPSD(simulation_id=1234, address=__GRIDAPPSD_URI__)
    print(utils.get_gridappsd_address())
    gridappsd_obj = GridAPPSD(1069573052, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())
goss_sim = "goss.gridappsd.process.request.simulation"
weather_channel = 'goss.gridappsd.process.request.data.timeseries'

def query_weather(start_time, end_time):
    query = {"queryMeasurement":"weather",
            "queryFilter":{"startTime":"1357048800000000",
                            "endTime":"1357058860000000"},
                            "responseFormat":"JSON"}
    query['queryFilter']['startTime'] = start_time
    query['queryFilter']['endTime'] = end_time

    weather_results = gridappsd_obj.get_response(weather_channel, query, timeout=220)

    if 'error' in weather_results and len(weather_results['error']['message']) > 1:
        return None
    time_dict = {int(row['time']): row for row in weather_results['data']}
    # time_dict = {}
    # for row in weather_results['data']:
    #     time_dict[int(row['time'])] = row

    weather_df = pd.DataFrame(time_dict).T
    return weather_df


def test1():

    start_time = 1374498000 + (0 * 60 * 60)
    # start_time = 1374411600 + (0 * 60 * 60)
    end_time = start_time + 3600 * 24
    # 7/22/13 7:00	7/22/13	7:00	35.1331078	59.5661794	11.4825329	75.938	19.6	12.8601	292.5
    weather_df = query_weather(start_time * 1000000, end_time * 1000000)
    # print(weather_df)
    # print(weather_df.head())
    current_sim_time = start_time
    temp_df = weather_df.iloc[weather_df.index.get_loc(current_sim_time, method='nearest')]
    print(temp_df)
    # 1/3/13 7:00	1/3/13	7:00	-0.009466324	-0.038115105	0.025649504	18.4262	34.17	11.7864	266.8
    # 1/3/13 13:00	1/3/13	13:00	47.3947001	95.066428	4.65688191	36.4478	16.01	7.6033	123.7
    start_time = 1357218000 + (0 * 60 * 60)  # (GMT): Thursday, January 3, 2013 1:00:00 PM
    end_time = start_time + 3600 * 24
    weather_df = query_weather(start_time * 1000000, end_time * 1000000)
    print(weather_df)
    # print(weather_df.head())
    current_sim_time = start_time
    temp_df = weather_df.iloc[weather_df.index.get_loc(current_sim_time, method='nearest')]
    print(temp_df)
    print(temp_df['DirectCH1'], type(temp_df['DirectCH1']))
    print("The DirectCH1 should be 47.3947001 not -0.0381151049")
    print("-0.0381151049 is the DirectCH1 for 1/3/13 7 AM MST")
    print("Check DirectCH1 == -0.0381151049 should be False")
    # print(temp_df['DirectCH1'] == -0.0381151)44
    print(temp_df['DirectCH1'] == -0.0381151049)
    print("Check DirectCH1 == 47.3947001 should be True")
    print(temp_df['DirectCH1'] == 47.3947001)

def test2():
    start_time = 1374489000 + (0 * 60 * 60)
    # start_time = 1374411600 + (0 * 60 * 60)
    end_time = start_time + 3600 * 24
    # 7/22/13 7:00	7/22/13	7:00	35.1331078	59.5661794	11.4825329	75.938	19.6	12.8601	292.5
    weather_df = query_weather(start_time * 1000000, end_time * 1000000)
    current_sim_time = start_time
    temp_df = weather_df.iloc[weather_df.index.get_loc(current_sim_time, method='nearest')]
    # print(temp_df)
    ghi = float(temp_df['DirectCH1']) / 100.
    print(ghi)
    weather_time = temp_df['time']
    temp_dict = {'type': 'X',
                 'time': weather_time,
                 'measurement': ghi}
    # print(temp_dict)
    next_time = 1374498026
    next_time = 1374489000
    # next_time = 1374519600
    # next_time = 1374500600
    temp_df = weather_df.iloc[weather_df.index.get_loc(next_time, method='nearest')]
    print(temp_df)
    print("DirectCH1 should be 84.1498419 not 0.0836627672")
    print(temp_df['DirectCH1'] == 84.1498419)
    ghi = temp_df['DirectCH1']
    print("DirectDH1 is " + str(ghi))

def test3():
    start_time = 1374233400 + (0 * 60 * 60)
    end_time = start_time + 3600 * 24
    weather_df = query_weather(start_time * 1000000, end_time * 1000000)
    current_sim_time = start_time
    temp_df = weather_df.iloc[weather_df.index.get_loc(current_sim_time, method='nearest')]
    # print(temp_df)
    expected_6 = {'Diffuse': 13.2182765, 'AvgWindSpeed': 8.6212, 'TowerRH': 40.51, 'long': '105.18 W', 'MST': '11:30', 'TowerDryBulbTemp': 78.8, 'DATE': '7/19/2013', 'DirectCH1': 86.88342440000001, 'GlobalCM22': 95.226216, 'AvgWindDirection': 69.76, 'time': 1374255000, 'place': 'Solar Radiation Research Laboratory', 'lat': '39.74 N'}
    expected_7 = {'Diffuse': 19.502496999999998, 'AvgWindSpeed': 6.3037, 'TowerRH': 43.19, 'long': '105.18 W', 'MST': '12:30', 'TowerDryBulbTemp': 78.962, 'DATE': '7/19/2013', 'DirectCH1': 82.712586, 'GlobalCM22': 99.261792, 'AvgWindDirection': 56.72, 'time': 1374258600, 'place': 'Solar Radiation Research Laboratory', 'lat': '39.74 N'}

    next_time = 1374233400
    # next_time = 1374233400 + 7 * 60*60

    print(next_time)
    temp_df = weather_df.iloc[weather_df.index.get_loc(next_time, method='nearest')]
    print(temp_df['DirectCH1'] - expected_6['DirectCH1'])
    print(temp_df['DirectCH1'] - expected_7['DirectCH1'])
    print(temp_df['DirectCH1'] == expected_6['DirectCH1'])
    print(temp_df['DirectCH1'] == expected_7['DirectCH1'])
    print(repr(temp_df.to_dict()))
    ghi = temp_df['DirectCH1']
    print("DirectDH1 is " + str(ghi))


# 1374483600
def test4():
    start_time = 1374483600 + (0 * 60 * 60)
    end_time = start_time + 3600 * 24
    weather_df = query_weather(start_time * 1000000, end_time * 1000000)
    current_sim_time = start_time
    temp_df = weather_df.iloc[weather_df.index.get_loc(current_sim_time, method='nearest')]
    # print(temp_df)
    expected_6 = {'Diffuse': 13.2182765, 'AvgWindSpeed': 8.6212, 'TowerRH': 40.51, 'long': '105.18 W', 'MST': '11:30', 'TowerDryBulbTemp': 78.8, 'DATE': '7/19/2013', 'DirectCH1': 86.88342440000001, 'GlobalCM22': 95.226216, 'AvgWindDirection': 69.76, 'time': 1374255000, 'place': 'Solar Radiation Research Laboratory', 'lat': '39.74 N'}
    expected_7 = {'Diffuse': 19.502496999999998, 'AvgWindSpeed': 6.3037, 'TowerRH': 43.19, 'long': '105.18 W', 'MST': '12:30', 'TowerDryBulbTemp': 78.962, 'DATE': '7/19/2013', 'DirectCH1': 82.712586, 'GlobalCM22': 99.261792, 'AvgWindDirection': 56.72, 'time': 1374258600, 'place': 'Solar Radiation Research Laboratory', 'lat': '39.74 N'}

    next_time = 1374483600
    # next_time = 1374233400 + 7 * 60*60

    print(next_time)
    temp_df = weather_df.iloc[weather_df.index.get_loc(next_time, method='nearest')]
    print(temp_df['DirectCH1'] - expected_6['DirectCH1'])
    print(temp_df['DirectCH1'] - expected_7['DirectCH1'])
    print(temp_df['DirectCH1'] == expected_6['DirectCH1'])
    print(temp_df['DirectCH1'] == expected_7['DirectCH1'])
    print(repr(temp_df.to_dict()))
    ghi = temp_df['DirectCH1']
    print("DirectDH1 is " + str(ghi))

# 1374423365
def test5():
    # 1248192060
    from datetime import datetime, timezone
    t = datetime.utcfromtimestamp(1248192060)
    adjusted_time = int(t.replace(year=2013).replace(tzinfo=timezone.utc).timestamp())
    start_time = adjusted_time + (0 * 60 * 60)
    end_time = adjusted_time + 3600 * 24
    _weather_df = query_weather(start_time * 1000000, end_time * 1000000)
    print(_weather_df)

    exit(0)

    start_time = 1374423365 + (0 * 60 * 60)
    end_time = start_time + 3600 * 24
    weather_df = query_weather(start_time * 1000000, end_time * 1000000)
    current_sim_time = start_time
    temp_df = weather_df.iloc[weather_df.index.get_loc(current_sim_time, method='nearest')]
    # print(temp_df)
    expected_6 = {'Diffuse': 13.2182765, 'AvgWindSpeed': 8.6212, 'TowerRH': 40.51, 'long': '105.18 W', 'MST': '11:30', 'TowerDryBulbTemp': 78.8, 'DATE': '7/19/2013', 'DirectCH1': 86.88342440000001, 'GlobalCM22': 95.226216, 'AvgWindDirection': 69.76, 'time': 1374255000, 'place': 'Solar Radiation Research Laboratory', 'lat': '39.74 N'}
    expected_7 = {'Diffuse': 19.502496999999998, 'AvgWindSpeed': 6.3037, 'TowerRH': 43.19, 'long': '105.18 W', 'MST': '12:30', 'TowerDryBulbTemp': 78.962, 'DATE': '7/19/2013', 'DirectCH1': 82.712586, 'GlobalCM22': 99.261792, 'AvgWindDirection': 56.72, 'time': 1374258600, 'place': 'Solar Radiation Research Laboratory', 'lat': '39.74 N'}

    next_time = 1374423365
    # next_time = 1374233400 + 7 * 60*60

    print(next_time)
    temp_df = weather_df.iloc[weather_df.index.get_loc(next_time, method='nearest')]
    print(temp_df['DirectCH1'] - expected_6['DirectCH1'])
    print(temp_df['DirectCH1'] - expected_7['DirectCH1'])
    print(temp_df['DirectCH1'] == expected_6['DirectCH1'])
    print(temp_df['DirectCH1'] == expected_7['DirectCH1'])
    print(repr(temp_df.to_dict()))
    ghi = temp_df['DirectCH1']
    print("DirectDH1 is " + str(ghi))

if __name__ == '__main__':
    #
    # test1()
    # #
    # test2()

    # test3()

    # test4()

    test5()

