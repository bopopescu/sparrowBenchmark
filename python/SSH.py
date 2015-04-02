import paramiko, subprocess
 
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect("50.112.163.194", username="ubuntu",allow_agent=False, key_filename = "matrix.pem")

stdin, stdout, stderr = client.exec_command("cd sparrow; java -XX:+UseConcMarkSweepGC -cp target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.daemon.SparrowDaemon -c sparrow.conf")

#data = stdout.read()

#print data

#print stderr.read()
raw_input()
client.close()

#subprocess.call("ssh -i matrix.pem ubuntu@50.112.163.194 ls", shell = True)
