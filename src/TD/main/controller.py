import inspect
import queue
import time
import sys
import subprocess
sys.path.append('D:\PythonProject\OpenCTP_TD')
sys.path.append('D:\ProgramData\Anaconda3\envs\CTPAPIDEV')
from src import config
import trade

if __name__ == "__main__":
    frontinfo=config.fronts["电信1"]["td"]
    user=config.user
    password=config.password
    authcode=config.authcode
    appid=config.appid
    brokerid=config.broker_id
    rootpath=config.rootpath
    tradetype="001"
    paraStr="CZCE,SA409,0,0,5,2250"
    cmd=['python','trade.py',frontinfo,user,password,authcode,appid,brokerid,rootpath,tradetype,paraStr]
    result=subprocess.run(cmd,capture_output=True)
    print(result.stdout.decode())
    if result.stderr:
        print(result.stderr.decode())

    #trade.main(frontinfo,user,password,authcode,appid,brokerid,rootpath,tradetype)