import time

from src.STRATEGY import Run_MA_Stragedy
import datetime
import schedule
import subprocess


class MD_Main():
    def __init__(self):
        self._job=""
        self._starttime1 = datetime.time(hour=8, minute=59, second=0)
        self._endtime1 = datetime.time(hour=15, minute=0, second=0)
        self._starttime2 = datetime.time(hour=20, minute=59, second=0)
        self._endtime2 = datetime.time(hour=23, minute=0, second=0)
        self._process=None
        print("-------行情启动开始-------")

    def Gen_MD(self):
        #print("job is runing----")
        time1 = datetime.datetime.now().time()
        print("time1 is：" + time1.strftime(("%H:%M:%S")))
        if (time1 >= self._starttime1 and time1<=self._endtime1):
            print("job is running")
            self._process=subprocess.Popen(['python', 'Run_MA_Stragedy.py'])
            #Run_MA_Stragedy.Main_Proc()  ####启动行情
        elif(time1>=self._starttime2 and time1<=self._endtime2):
            print("job is running")
            self._process = subprocess.Popen(['python', 'Run_MA_Stragedy.py'])
            #Run_MA_Stragedy.Main_Proc()  ####启动行情
        else:
            print("job is stoping")
            self._process.terminate()
            self._process.wait()
            #schedule.cancel_job(self._job)
            #Run_MA_Stragedy.Exit_Proc()
        return schedule.CancelJob

    def Control_Gen_MD(self):
        #print("Ave_Mode_1 is starting")
        self._job = schedule.every(1).minutes.do(self.Gen_MD)

    def Run_Gen_MD(self):
        print("--------时间调度开始-----------")
        schedule.every().day.at(self._starttime1.strftime("%H:%M:%S")).do(self.Control_Gen_MD)
        schedule.every().day.at(self._endtime1.strftime("%H:%M:%S")).do(self.Control_Gen_MD)
        schedule.every().day.at(self._starttime2.strftime("%H:%M:%S")).do(self.Control_Gen_MD)
        schedule.every().day.at(self._endtime2.strftime("%H:%M:%S")).do(self.Control_Gen_MD)
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    md_main=MD_Main()
    md_main.Run_Gen_MD()


