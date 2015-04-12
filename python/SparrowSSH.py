import paramiko, subprocess


workerCommand = "cd sparrow/python;python SparrowLocal.py -l all -nw "
killWorkerCommand = "cd sparrow/python;python SparrowLocal.py -k all"
updateIPCommand = "cd sparrow/python;python SparrowConfigC.py add "
setIPCommand = "cd sparrow/python;python SparrowConfigC.py set "
grabBackendResultsCommand = "cd sparrow;cat ResultsBackend.txt"
grabSchedulingResultsCommand = "cd sparrow;cat ResultsScheduling.txt"


def startWorker(ip, configidx):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username="ubuntu",allow_agent=False, key_filename = "matrix.pem")

    stdin, stdout, stderr = client.exec_command(workerCommand + str(configidx))
    
    client.close()

def killWorker(ip):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username="ubuntu",allow_agent=False, key_filename = "matrix.pem")

    stdin, stdout, stderr = client.exec_command(killWorkerCommand)
    
    client.close()

def grabResults(fp, ip):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username="ubuntu",allow_agent=False, key_filename = "matrix.pem")

    stdin, stdout, stderr = client.exec_command(grabBackendResultsCommand)
    for line in stdout.readlines():
        fp.write(line)
    
    stdin, stdout, stderr = client.exec_command(grabSchedulingResultsCommand)
    for line in stdout.readlines():
        fp.write(line)
        
    client.close()
    
def setClientConfigIps(ipsToStore, workerIps):
    for ip in workerIps:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip, username="ubuntu",allow_agent=False, key_filename = "matrix.pem")

        stdin, stdout, stderr = client.exec_command(setIPCommand + " ".join(tuple(ipsToStore)))
        print stdout.read()
        print stderr.read()
    
        client.close()
    
def updateClientConfigIps(additionalIps, workerIps):
    for ip in workerIps:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip, username="ubuntu",allow_agent=False, key_filename = "matrix.pem")

        stdin, stdout, stderr = client.exec_command(updateIPCommand + " ".join(tuple(additionalIps)))
       
        client.close()
    
if __name__ == "__main__":

    ip = "54.148.243.154"
    setClientConfigIps(["1.1.1.1", u'52.11.116.188', "2.2.2.2"], [ip])
  
#    client = paramiko.SSHClient()
#    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#    client.connect("52.11.154.179", username="ubuntu",allow_agent=False, key_filename = "matrix.pem")

#    print "connected"
    #stdin, stdout, stderr = client.exec_command("nohup python /home/ubuntu/sparrow/python/SparrowLocal.py -l client &")
#    stdin, stdout, stderr = client.exec_command("cd sparrow/python;python SparrowLocal.py -l all")

    #raw_input()
    #data = stdout.read()

    #print data

    #print stderr.read()
#    client.close()

