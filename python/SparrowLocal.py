## kill/launch worker/sparrow

import argparse, json, os, shlex, subprocess, sys, time
from localpath import *

workerCommand = "java -cp target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.examples.BBackend -c Conf/conf.Backend"
clientCommand = "java -XX:+UseConcMarkSweepGC -cp target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.daemon.SparrowDaemon -c sparrow.conf"


#SparrowPID file stores [CLIENT_PID,WORKER_PID]

#program is one of {"worker","client","all"}
def launch(program, workerConfigNumber):
    
    pidFile = open('SparrowPID', 'r')
    s = json.load(pidFile)
    pidFile.close()

    pidFile = open('SparrowPID', 'w')
    if program != "worker": # => program = all or client => launch client 
        if s[0] != 0:
            print "SparrowLocal - Client already running at launch request. Kill previous Client"
            subprocess.call("kill " + str(s[0]), shell = True)
        
        sparrowClient = subprocess.Popen(shlex.split(clientCommand))
        time.sleep(2)
        s[0] = sparrowClient.pid

    if program != "client": # => program = all or worker => launch worker
        if s[0] == 0:
            print "SparrowLocal - Client not running at worker launch request. Launching a new Client"
            sparrowClient = subprocess.Popen(shlex.split(clientCommand))
            time.sleep(2)
            s[0] = sparrowClient.pid
            
        sparrowWorker = subprocess.Popen(shlex.split(workerCommand + workerConfigNumber))
        s[1] = sparrowWorker.pid 
    	
    json.dump(s,pidFile)	
    pidFile.close()

#victim is one of {"worker","client","all"} 
def s_kill(victim):	
    pidFile = open('SparrowPID', 'r')
    s = json.load(pidFile)
    pidFile.close()

    pidFile = open('SparrowPID', 'w')
    if victim != "client": # => victim = all or worker => kill worker
        subprocess.call("kill " + str(s[1]), shell = True)
        s[1] = 0 

    if victim != "worker": # => victim = all or client => kill client
        if s[1] != 0:
            print "SparrowLocal - Worker still running at Client kill request. Killing worker first"
            subprocess.call("kill " + str(s[1]), shell = True)
            s[1] = 0 
        subprocess.call("kill " + str(s[0]), shell = True)		
        s[0] = 0 
        
    json.dump(s,pidFile)
    pidFile.close()
	
	
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "launch/kill worker/sparrow client", prog = "SparrowLocal.py")
    parser.add_argument("-k", "--kill", choices = ['worker','client','all'], default = None)
    parser.add_argument("-l", "--launch", choices = ['worker','client','all'], default = None)
    parser.add_argument("-r", "--reset", action = "store_true", default = False)
    parser.add_argument("-nw", "--numberworker", default = "")
    
    
    args = parser.parse_args() 
    
    os.chdir(pwd)

    if args.reset:
        pidFile = open('SparrowPID', 'w')
        json.dump([0,0], pidFile)
        pidFile.close()
    if args.kill != None:
        s_kill(args.kill)
    if args.launch != None:
        launch(args.launch, args.numberworker)
    sys.exit(0)
    
