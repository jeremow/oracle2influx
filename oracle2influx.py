# -*- coding: utf-8 -*-
# oracle2influx.py
# Author: Jeremy
# Description: Client Seedlink adapté à MONA DASH

import argparse

import obspy
import time

from config import *
from utils import *
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

        print(self.stations)

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

        self.xat_timestamp = None
        self.soh1_timestamp = None
        self.soh2_timestamp = None
        self.soh3_timestamp = None

    def write_data_influx(self):
        try:
            for sta in self.stations:
                t_start = obspy.UTCDateTime()
                data = []
                self.cursor.execute(f'SELECT * FROM {TABLE_ORACLE_XAT} WHERE '
                                    f'STATION_NAME=:sta AND IS_DATA=:data ORDER BY TIME DESC',
                                    sta=sta, data='Data')

                for row in self.cursor:

                    timestamp = int(row[0].timestamp() * 1e3)

                    sensor1_value_base2 = base10_to_base2_str(row[19])

                    data.append({
                        "measurement": "HEALTH_STATES_XAT",
                        "tags": {"location": sta},
                        "fields": {
                            "vault0_temperature": float(row[3] if row[3] is not None else 0.0),
                            "vault0_humidity": float(row[4] if row[4] is not None else 0.0),
                            "vault1_temperature": float(row[5]) if row[5] is not None else 0.0,
                            "vault1_humidity": float(row[6]) if row[6] is not None else 0.0,
                            "seismometer_temperature": float(row[7]) if row[7] is not None else 0.0,
                            "outside_temperature": float(row[8]) if row[8] is not None else 0.0,
                            "vpn_voltage": float(row[9]) if row[9] is not None else 0.0,
                            "vpn_current": float(row[10]) if row[10] is not None else 0.0,
                            "telemeter_voltage": float(row[11]) if row[11] is not None else 0.0,
                            "telemeter_current": float(row[12]) if row[12] is not None else 0.0,
                            "digitizer_voltage": float(row[13]) if row[13] is not None else 0.0,
                            "digitizer_current": float(row[14]) if row[14] is not None else 0.0,
                            "computer_voltage": float(row[15]) if row[15] is not None else 0.0,
                            "computer_current": float(row[16]) if row[16] is not None else 0.0,
                            "device_voltage": float(row[20]) if row[20] is not None else 0.0,
                            "device_current": float(row[21]) if row[21] is not None else 0.0,
                            "saxiul": int(sensor1_value_base2[0]),
                            "water_presence": int(sensor1_value_base2[1]),
                            "door_1": int(sensor1_value_base2[2]),
                            "door_2": int(sensor1_value_base2[3]),
                            "loop": int(sensor1_value_base2[5]),
                        },
                        "time": timestamp
                    })

                    break  # to leave the current cursor after getting the last value

                self.cursor.execute(f'SELECT * FROM {TABLE_ORACLE_SOH[0]} WHERE STATION=:sta ORDER BY DATE1 DESC',
                                    sta=sta)
                for row in self.cursor:
                    timestamp = int(row[1].timestamp() * 1e3)

                    data.append({
                        "measurement": f"HEALTH_STATES_{TABLE_ORACLE_SOH[0]}",
                        "tags": {"location": sta},
                        "fields": {
                            "used_disksize": int(row[2]),
                            "available_disksize": int(row[3]),
                            "total_disksize": int(row[4]),

                        },
                        "time": timestamp
                    })

                    break  # to leave the current cursor after getting the last value

                self.cursor.execute(f'SELECT * FROM {TABLE_ORACLE_SOH[1]} WHERE STATION=:sta ORDER BY DATE1 DESC',
                                    sta=sta)
                for row in self.cursor:
                    timestamp = int(row[1].timestamp() * 1e3)

                    data.append({
                        "measurement": f"HEALTH_STATES_{TABLE_ORACLE_SOH[1]}",
                        "tags": {"location": sta},
                        "fields": {
                            "e_massposition": float(row[2]),
                            "n_massposition": float(row[3]),
                            "z_massposition": float(row[4]),

                        },
                        "time": timestamp
                    })

                    break  # to leave the current cursor after getting the last value

                self.cursor.execute(f'SELECT * FROM {TABLE_ORACLE_SOH[2]} WHERE STATION=:sta ORDER BY DATE1 DESC',
                                    sta=sta)
                for row in self.cursor:
                    timestamp = int(row[3].timestamp() * 1e3)

                    data.append({
                        "measurement": f"HEALTH_STATES_{TABLE_ORACLE_SOH[2]}",
                        "tags": {"location": sta},
                        "fields": {
                            "battery_voltage": float(row[1]),
                            "temperature": float(row[2]),
                        },
                        "time": timestamp
                    })

                    break

                # self.cursor.execute(f'SELECT * FROM {TABLE_ORACLE_XAT} WHERE '
                #                     f'STATION_NAME=:sta AND IS_DATA=:data ORDER BY TIME DESC',
                #                     sta=sta, data='Alarm')
                # for row in self.cursor:
                #     timestamp = row[0]
                #     alarm = row[20]
                #     if alarm is not None:
                #         self.analyze_alarm(sta, alarm, dt)
                #     break  # to leave the current cursor after getting the last value
                try:

                    self.write_api.write(self.bucket, self.org, record=data, write_precision=WritePrecision.MS)
                    t_stop = obspy.UTCDateTime()
                    print(f'Oracle data of {sta} sent to {self.bucket} in {t_stop - t_start}s')
                except Exception as e:
                    print(e)
                    print(f'Oracle data of {sta} not sent to {self.bucket}.')
                    pass

            return True

        except cx_Oracle.DatabaseError:
            print('Request is false.')
            return False

    def run(self):
        while True:

            response = self.write_data_influx()
            if response is True:
                time.sleep(300)
            else:
                print('Something wrong went with Oracle Database. Retry sending data in 30s.')
                time.sleep(30)


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
    parser = argparse.ArgumentParser(description='Query data from a Oracle and write the data into influxdb v2',
                                     formatter_class=argparse.RawTextHelpFormatter)

    # Script functionalities
    parser.add_argument('-s', '--oracle-host', help='Path to Oracle server', required=True)
    parser.add_argument('-p', '--oracle-port', help='Port of Oracle server', default='1522')
    parser.add_argument('-S', '--server-influx', help='Path of influx server', required=True)
    parser.add_argument('-P', '--port-influx', help='Port of influx server', default='8086')
    parser.add_argument('-b', '--bucket', help='Name of the bucket', required=True)
    parser.add_argument('-o', '--org', help='Name of the organization', required=True)
    parser.add_argument('-t', '--token', help='Token authorization of influxdb', required=True)
    # parser.add_argument('-m', '--mseed', help='Path to mseed data folder', required=True)

    args = parser.parse_args()

    print(f'Server Oracle: {args.oracle_host} ; Port: {args.oracle_port}')
    print(f'Server Influx: {args.server_influx} ; Port: {args.port_influx}')
    print("--------------------------\n"
          "Starting Oracle server and verifying Influx connection...")

    return args


if __name__ == '__main__':
    args = get_arguments()
    init_oracle_client(CLIENT_ORACLE)

    client = OracleInfluxClient(args.oracle_host, args.oracle_port, args.server_influx + ':' + args.port_influx,
                                args.bucket, args.token, args.org)

    client.run()
