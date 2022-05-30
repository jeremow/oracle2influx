# ORACLE CLIENT
CLIENT_ORACLE: str = r'/u01/app/oracle/product/19.3.0/dbhome_1/lib'

# ORACLE CLIENT
# HOST_ORACLE: str = '192.168.1.76'
# PORT_ORACLE: str = '1522'
SERVICE_ORACLE: str = 'hatdb2'
USER_ORACLE: str = r'hat'
PWD_ORACLE: str = 'mndc_iag'

# for TABLE_ORACLE_XAT and TABLE_ORACLE_SOH, you have to be careful about the class OracleClient in the method
# verify_states if the names are still matching with your database columns
TABLE_ORACLE_XAT: str = 'hatv4'
TABLE_ORACLE_SOH: list = ['DISKSIZE1', 'MASS_POSITION', 'BATTERYVOLTAGE']

# ALARM XAT
XAT_ALARM_NAME: list = ['Loop', 'Water', 'Door 1', 'Door 2']
XAT_NORMAL_STATE: dict = {
    'UB4M': '1100',
    'TEST': '1000',
    'CCBM': '1000',
}

WAIT_DURATION: int = 300
