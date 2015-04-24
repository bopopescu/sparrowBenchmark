import subprocess, os, shlex, time, sys

os.chdir("/home/ubuntu/sparrow-master/python")
subprocess.call(shlex.split("echo \"\" > Finish.txt"), shell = True)
commandFrontend = "java -cp ../target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.examples.BFrontend -c ../Conf/conf.Frontend1"

startTime = time.time()
run = True
while run and time.time() - startTime < 600:
    #wait for front end to finish
    time.sleep(5)
    print "Benchmark - check Finish.txt"
    fd = open("Finish.txt", "r")
    if fd.readline() == "Experience finished\n":
        run = False
        break
    fd.close()

sparrowFrontend.kill()

print "Experience ended?" + str(not(run))

if run:
    sys.exit(1)
else:
    sys.exit(0)
