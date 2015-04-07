import shlex, SparrowDistant, SparrowSSH, string, subprocess, time 

#SparrowBenchmark

#One benchmark is represented by 
    #[index of frontend config, index of backend config, number of workers]

commandFrontend = "java -cp target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.examples.BFrontend -c Conf/conf.Frontend"
frontendPrivateIp = "FrontendprivateIp"
benchmarks = [[1,1,1]]
benchmarks = sorted(benchmarks, key=lambda benchmark: benchmark[2])
workerPrivateIps = []
workerIps = []
backendConfs = [x[1] for x in benchmarks]

pushConfigFilesP1 = "scp -i matrix.pem Conf/conf.Backend"
user = " ubuntu@ec2-"
AwsDns = ".us-west-2.compute.amazonaws.com:"

#scp -i matrix.pem test.py ubuntu@ec2-52-11-154-179.us-west-2.compute.amazonaws.com:~/
instanceLauncher = SparrowDistant.SparrowDistant()
print "SPARROW BENCHMARKING"

for benchmark in benchmarks:
    lackingInstances = benchmark[2] - len(instanceLauncher.instances)
    privateIps, ips = instanceLauncher.(lackingInstances, lackingInstances*30)
    
    #store configs
    resultsFp = open("Results.txt", a)
    confFile = open("Conf/conf.Frontend"+benchmark[1],r)
    content = confFile.read()
    count = content.count("\n")
    resultsFp.write("\nFrontend config," + content.replace("\n", ",", count-1))
    confFile = open("Conf/conf.Backend"+benchmark[1],r)
    content = confFile.read()
    count = content.count("\n")
    resultsFp.write("BackendConfig," + content.replace("\n", ",", count-1))
    resultsFp.write("number of workers," + benchmark[2] + "\n")
    resultsFp.close()
    
    raw_input("storeconfig")
    #copy conf files on new workers
    for ip in ips:
        scpIp = string.replace(ip, ".", "-")
        for backendConf in backendConfs:
            subprocess.call(shlex.split(pushConfigFilesP1 + str(backendConf) + user + scpIp + AwsDns))
            
    backendConfs.pop(0)
    #List of IPs update/set
      #update already operational workers -add "privateIps"
    SparrowSSH.updateClientConfigIps(privateIps, workerIps)
    
      #update the total list of ips
    workerPrivateIps += privateIps
    workerIps += ips
    
      #set conf file of new workers
    SparrowSSH.setClientConfigIps(frontendPrivateIp + workerPrivateIps, ips)
    raw_input("worker conf set")
    #Launch worker/client on instance
    for ip in workerIps
        SparrowSSH.startWorker(ip, benchmark[1])        

    print "5s delay for start ups"
    time.sleep(5)
    raw_input("worker launched")
    sparrowFrontend = supbrocess.Popen(shlex.split(commandFrontend+benchmark[0]))
    
    while time.clock - startTime < Max delay:
        #wait for front end to finish
        time.sleep(5)
        fd = open("Finish.txt", "r")
        if fd.readline() == "Experience finished":
            run = False
            print "Experience ended"
        fd.close()
    raw_input("exp end")
    resultsFp = open("Results.txt", a)
        
    #Kill worker/client on instance
    for ip in workerIps:
        SparrowSSH.killWorker(ip)
        #fetch all results
        SparrowSSH.grabResults(resultsFp, ip)
        
    resultsFp.close()

instanceLauncher.terminateAll()    

