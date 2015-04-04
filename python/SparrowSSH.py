import paramiko, subprocess


workerCommand = "cd sparrow/python;python SparrowLocal.py -l all"
updateIPCommand = "cd sparrow/python;python SparrowConfigC.py"
grabBackendResultsCommand = "cd sparrow;cat ResultsBackend.txt"
grabSchedulingResultsCommand = "cd sparrow;cat ResultsScheduling.txt"
portNumber = "20502"

def updateIPs(ip_list):
    ipPortList = [":".join((x,portNumber)) for x in ip_list]
    for ip in ip_list:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip, username="ubuntu",allow_agent=False, key_filename = "matrix.pem")

        stdin, stdout, stderr = client.exec_command(" ".join((updateIPCommand,) + tuple(ipPortList)))

        client.close()

def startWorker(ip):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username="ubuntu",allow_agent=False, key_filename = "matrix.pem")

    stdin, stdout, stderr = client.exec_command(workerCommand)
    
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
    

if __name__ == "__main__":

    ip = "52.11.154.179"
    fp = open("testResults.txt", "a")
    grabResults(fp,ip)    
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

