import os, shlex, SparrowDistant, SparrowConfigC, SparrowSSH, string, subprocess, time 

#SparrowBenchmark

#One benchmark is represented by 
    #[index of frontend config, index of backend config, number of workers]
import time

commandFrontend = "java -cp ../target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.examples.BFrontend -c ../Conf/conf.Frontend"
frontendPrivateIp = "172.31.22.75"
benchmarks = [[1,1,1]]
benchmarks = sorted(benchmarks, key=lambda benchmark: benchmark[2])
workerPrivateIps = [frontendPrivateIp]
workerIps = []
backendConfs = [x[1] for x in benchmarks]

pushConfigFilesP1 = "scp -i matrix.pem ../Conf/conf.Backend"
user = " ubuntu@ec2-"
AwsDns = ".us-west-2.compute.amazonaws.com:~/sparrow/Conf/"

#scp -i matrix.pem test.py ubuntu@ec2-52-11-154-179.us-west-2.compute.amazonaws.com:~/
instanceLauncher = SparrowDistant.SparrowDistant()
print "---SPARROW BENCHMARKING---\n"

SparrowConfigC.setIps([frontendPrivateIp])
for benchmark in benchmarks:    
    os.chdir("/home/ubuntu/sparrow/python")
    subprocess.call(shlex.split("echo "" > ../Finished.txt"), shell = True)
    lackingInstances = benchmark[2] - len(instanceLauncher.instances)
    privateIps, ips = instanceLauncher.launchInstances(lackingInstances, lackingInstances*30)
    
    #store configs
    resultsFp = open("Results.txt", "a")
    confFile = open("../Conf/conf.Frontend"+str(benchmark[1]),"r")
    content = confFile.read()
    count = content.count("\n")
    resultsFp.write("\nFrontend config," + content.replace("\n", ",", count-1))
    confFile = open("../Conf/conf.Backend"+str(benchmark[1]),"r")
    content = confFile.read()
    count = content.count("\n")
    resultsFp.write("BackendConfig," + content.replace("\n", ",", count-1))
    resultsFp.write("number of workers," + str(benchmark[2]) + "\n")
    resultsFp.close()
    
    #raw_input("storeconfig")
    print "start sleep"
    time.sleep(60)
    print "end sleep"
    
    #copy conf files on new workers
    os.chdir("/home/ubuntu/sparrow/python")
    for ip in ips:
        print "updating " + ip
        scpIp = string.replace(ip, ".", "-")
        for backendConf in backendConfs:
            print "  conffile " + str(backendConf)
            subprocess.call(shlex.split(pushConfigFilesP1 + str(backendConf) + user + scpIp + AwsDns))
            
    backendConfs.pop(0)
    #List of IPs update/set
      #update already operational workers -add "privateIps" + frontend instance
    SparrowSSH.updateClientConfigIps(privateIps, workerIps)
    SparrowConfigC.addIps(privateIps)
      #update the total list of ips
    workerPrivateIps += privateIps
    workerIps += ips
    print "workerPrivateIps = " + str(workerPrivateIps)
      #set conf file of new workers
    print str(workerPrivateIps) + " " + str(ips)
    os.chdir("/home/ubuntu/sparrow/python")
    SparrowSSH.setClientConfigIps(workerPrivateIps, ips)
    raw_input("config pushed, press enter")    
    #raw_input("worker conf set")
    #Launch worker/client on instance
    subprocess.call(shlex.split("python SparrowLocal.py -l all -nw " + str(benchmark[1])))
    for ip in workerIps:
        SparrowSSH.startWorker(ip, benchmark[1])        

    print "30s delay for start ups"
    time.sleep(30)
    raw_input("worker launched press enter\n")
    #subprocess.call(shlex.split("python SparrowLocal.py -r"))
    #subprocess.call(shlex.split("python SparrowLocal.py -l client"))
    sparrowFrontend = subprocess.Popen(shlex.split(commandFrontend+str(benchmark[0])))
    
    os.chdir("/home/ubuntu/sparrow/python")
    startTime = time.time()
    run = True
    while run and time.time() - startTime < 300:
        #wait for front end to finish
        time.sleep(5)
        print "Benchmark - check Finish.txt"
        fd = open("../Finish.txt", "r")
        if fd.readline() == "Experience finished":
            run = False
            print "Experience ended"
        fd.close()
        
    raw_input("exp end, press enter")
    resultsFp = open("../Results.txt", "a")
        
    #Kill worker/client on instances
    
    for ip in workerIps:
        SparrowSSH.killWorker(ip)
        #fetch all results
        SparrowSSH.grabResults(resultsFp, ip)
        
    resultsFp.close()
    
    sparrowFrontend.kill()
    subprocess.call(shlex.split("python SparrowLocal.py -k all"))
print "End of Benchmarking"

#instanceLauncher.terminateAll()    

