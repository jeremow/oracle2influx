# -*- coding: utf-8 -*-
# oracle2influx.py
# Author: Jeremy
# Description: Client Seedlink adapté à MONA DASH

import argparse

import obspy

from config import *
import cx_Oracle

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision


class OracleInfluxClient:
    def __init__(self, oracle_host, oracle_port, server_influx_url, bucket, token, org):

        self.dsn_tns = cx_Oracle.makedsn(oracle_host, oracle_port, service_name=SERVICE_ORACLE)
        try:
            self.conn = cx_Oracle.connect(user=USER_ORACLE, password=PWD_ORACLE, dsn=self.dsn_tns)
            self.cursor = self.conn.cursor()
            self.stations = []
            self.cursor.execute(f'SELECT STATION_NAME FROM {TABLE_ORACLE_XAT} GROUP BY STATION_NAME')
            for row in self.cursor:
                self.stations.append(row[0])

            for TABLE in TABLE_ORACLE_SOH:
                self.cursor.execute(f'SELECT STATION FROM {TABLE} GROUP BY STATION')
                for row in self.cursor:
                    sta = row[0]
                    if sta not in self.stations:
                        self.stations.append(sta)

            print('Connection successful to Oracle Client.')
        except cx_Oracle.ProgrammingError:
            print('Connection error for the Oracle Client')
        except cx_Oracle.DatabaseError:
            print('Connection error for the Oracle Client')

        self.server_influx = server_influx_url
        self.bucket = bucket
        self.token = token
        self.org = org
        self.client_influx = InfluxDBClient(url=self.server_influx, token=self.token, org=self.org)

        if self.client_influx.ping() is True:
            print("Connection success to InfluxDB.")
        else:
            print(f"Connection failure to InfluxDB.")
            exit(1)

        self.write_api = self.client_influx.write_api(SYNCHRONOUS)

    def write_data_influx(self):
        try:
            for sta in self.stations:

                self.cursor.execute(f'SELECT * FROM {TABLE_ORACLE_XAT} WHERE '
                                    f'STATION_NAME=:sta AND IS_DATA=:data ORDER BY TIME DESC',
                                    sta=sta, data='Data')
                for row in self.cursor:

                    timestamp = row[0]

                    vault0_temperature = row[3]
                    vault0_humidity = row[4]
                    vault1_temperature = row[5]
                    vault1_humidity = row[6]
                    seismometer_temperature = row[7]
                    outside_temperature = row[8]
                    vpn_voltage = row[9]
                    vpn_current = row[10]
                    telemeter_voltage = row[11]
                    telemeter_current = row[12]
                    digitizer_voltage = row[13]
                    digitizer_current = row[14]
                    computer_voltage = row[15]
                    computer_current = row[16]
                    device_voltage = row[20]
                    device_current = row[21]
                    sensor1_value_base2 = base10_to_base2_str(row[19])
                    sensor1_value = [sensor1_value_base2[0], sensor1_value_base2[1],
                                     'open' if sensor1_value_base2[2] == '1' else 'close',
                                     'open' if sensor1_value_base2[3] == '1' else 'close', sensor1_value_base2[4],
                                     sensor1_value_base2[5], sensor1_value_base2[6], sensor1_value_base2[7]]


                    states_data += f"""

                    <state name='Saxiul XAT' datetime='{dt}' value='{sensor1_value[0]}' problem='0'/>
                    <state name='Water XAT' datetime='{dt}' value='{sensor1_value[1]}' problem='0'/>
                    <state name='Door 1 XAT' datetime='{dt}' value='{sensor1_value[2]}' problem='0'/>
                    <state name='Door 2 XAT' datetime='{dt}' value='{sensor1_value[3]}' problem='0'/>
                    <state name='Loop XAT' datetime='{dt}' value='{sensor1_value[5]}' problem='0'/>
                    """

                    break  # to leave the current cursor after getting the last value

                self.cursor.execute(f'SELECT * FROM {TABLE_ORACLE_SOH[0]} WHERE STATION=:sta ORDER BY DATE1 DESC',
                                    sta=sta)
                for row in self.cursor:
                    timestamp = row[1]
                    used_disksize = row[2]
                    available_disksize = row[3]
                    total_disksize = row[4]

                    break  # to leave the current cursor after getting the last value

                self.cursor.execute(f'SELECT * FROM {TABLE_ORACLE_SOH[1]} WHERE STATION=:sta ORDER BY DATE1 DESC',
                                    sta=sta)
                for row in self.cursor:
                    timestamp = row[1]
                    e_massposition = row[2]
                    n_massposition = row[3]
                    z_massposition = row[4]


                    break  # to leave the current cursor after getting the last value

                self.cursor.execute(f'SELECT * FROM {TABLE_ORACLE_SOH[2]} WHERE STATION=:sta ORDER BY DATE1 DESC',
                                    sta=sta)
                for row in self.cursor:
                    timestamp = row[3]
                    battery_voltage = row[1]
                    temperature = row[2]


                    break

                states_data += f"</station>"

                self.cursor.execute(f'SELECT * FROM {TABLE_ORACLE_XAT} WHERE '
                                    f'STATION_NAME=:sta AND IS_DATA=:data ORDER BY TIME DESC',
                                    sta=sta, data='Alarm')
                # for row in self.cursor:
                #     timestamp = row[0]
                #     alarm = row[20]
                #     if alarm is not None:
                #         self.analyze_alarm(sta, alarm, dt)
                #     break  # to leave the current cursor after getting the last value


        except cx_Oracle.DatabaseError:
            print('Request is false.')

        # tr.resample(sampling_rate=25.0)
        t_start = obspy.UTCDateTime()
        tr.detrend(type='constant')

        if tr is not None:
            if tr.stats.location == '':
                station = tr.stats.network + '.' + tr.stats.station + '.' + tr.stats.channel
            else:
                station = tr.stats.network + '.' + tr.stats.station + '.' + tr.stats.location + '.' + tr.stats.channel

            data = []
            timestamp_start = int(tr.stats.starttime.timestamp * 1e3)
            for i, seismic_point in enumerate(tr.data):
                timestamp = timestamp_start + i * int(tr.stats.delta * 1e3)
                data.append({
                    "measurement": "SEISMIC_DATA",
                    "tags": {"location": station},
                    "fields": {
                        "trace": int(seismic_point),
                    },
                    "time": timestamp
                })

            self.write_api.write(self.bucket, self.org, record=data, write_precision=WritePrecision.MS)
            t_stop = obspy.UTCDateTime()

            print(f'{station} sent to {self.bucket} in {t_stop-t_start}s')

        else:
            print("blockette contains no trace")

    def run(self):
        for station in self.streams:
            full_sta_name = station.split('.')
            net = full_sta_name[0]
            sta = full_sta_name[1]
            cha = full_sta_name[2] + full_sta_name[3]
            self.select_stream(net, sta, cha)
        while True:

            data = self.conn.collect()

            if data == SLPacket.SLTERMINATE:
                self.on_terminate()
                continue
            elif data == SLPacket.SLERROR:
                self.on_seedlink_error()
                continue

            # At this point the received data should be a SeedLink packet
            # XXX In SLClient there is a check for data == None, but I think
            #     there is no way that self.conn.collect() can ever return None
            assert(isinstance(data, SLPacket))

            packet_type = data.get_type()

            # Ignore in-stream INFO packets (not supported)
            if packet_type not in (SLPacket.TYPE_SLINF, SLPacket.TYPE_SLINFT):
                # The packet should be a data packet
                trace = data.get_trace()
                # Pass the trace to the on_data callback
                self.on_data(trace)

    def on_terminate(self):
        self._EasySeedLinkClient__streaming_started = False
        self.close()

        del self.conn
        self.conn = SeedLinkConnection(timeout=30)
        self.conn.set_sl_address('%s:%d' %
                                 (self.server_hostname, self.server_port))
        self.connect()

    def on_seedlink_error(self):
        self._EasySeedLinkClient__streaming_started = False
        self.close()
        self.streams = self.conn.streams.copy()
        del self.conn
        self.conn = SeedLinkConnection(timeout=30)
        self.conn.set_sl_address('%s:%d' %
                                 (self.server_hostname, self.server_port))
        self.conn.streams = self.streams.copy()
        self.run()


# class SLThread(Thread):
#     def __init__(self, name, client):
#         Thread.__init__(self)
#         self.name = name
#         self.client = client
#
#     def run(self):
#         print('Starting Thread ', self.name)
#         print('Server: ', self.client.server_hostname)
#         print('Port:', self.client.server_port)
#         print("--------------------------\n")
#         print('Network list:', self.client.network_list_values)
#         self.client.run()
#
#     def close(self):
#         self.client.close()

def init_oracle_client(path_to_client):
    print(f'Initializing Oracle client to {path_to_client}')
    try:
        cx_Oracle.init_oracle_client(path_to_client)
    except cx_Oracle.DatabaseError as e:
        print(e)
        print(f"Variable CLIENT_ORACLE for the Oracle Client software not/badly configured in config.py.\n"
              f"Value: {path_to_client}.")


def get_arguments():
    """returns AttribDict with command line arguments"""
    parser = argparse.ArgumentParser(
        description='Launch a seedlink  and write the data into influxdb v2',
        formatter_class=argparse.RawTextHelpFormatter)

    # Script functionalities
    parser.add_argument('-s', '--server-sl', help='Path to SL server', required=True)
    parser.add_argument('-p', '--port-sl', help='Port of the SL server')
    parser.add_argument('-S', '--server-influx', help='Path of influx server', required=True)
    parser.add_argument('-P', '--port-influx', help='Port of influx server')
    parser.add_argument('-b', '--bucket', help='Name of the bucket', required=True)
    parser.add_argument('-o', '--org', help='Name of the organization', required=True)
    parser.add_argument('-t', '--token', help='Token authorization of influxdb', required=True)
    # parser.add_argument('-m', '--mseed', help='Path to mseed data folder', required=True)

    args = parser.parse_args()

    if args.port_sl is None:
        args.port_sl = '18000'
    if args.port_influx is None:
        args.port_influx = '8086'

    print(f'Server SL: {args.server_sl} ; Port: {args.port_sl}')
    print(f'Server Influx: {args.server_influx} ; Port: {args.port_influx}')
    print("--------------------------\n"
          "Starting Seedlink server and verifying Influx connection...")

    return args


if __name__ == '__main__':
    args = get_arguments()
    init_oracle_client(CLIENT_ORACLE)

    client = OracleInfluxClient(args.server_sl + ':' + args.port_sl, args.server_influx + ':' + args.port_influx,
                                  args.bucket, args.token, args.org)

    client.run()
