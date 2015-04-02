import boto, boto.ec2, time
from AWSconf import*

class SparrowDistant:


    def launchInstances(self, number, timeOut):
        reservation = self.EC2.run_instances(image_id= AMI, instance_type = 't2.micro', key_name= sshKey, min_count = number, max_count = number)
        notLaunched = range(number)
        ips = []
        toLaunch = number
        time.sleep(1)
        startTime = time.clock()

        while toLaunch > 0 and (time.clock() - startTime) < timeOut:
            print "".join(("SparrowDistant - still ", str(toLaunch), " instances to launch")) 

            idx = notLaunched.pop(0)
            update = reservation.instances[idx].update()
            if update == "running":
                print reservation.instances[idx].ip_address
                ips.append(reservation.instances[idx].ip_address)
                self.instances.append(reservation.instances[idx])
                toLaunch -= 1
            else:
                notLaunched.append(idx)
                time.sleep(5)
        
        if toLaunch == 0:
            print "".join(("SparrowDistant - ", str(number), " instances launched in ", str(time.clock() - startTime), " seconds"))
        else:
            print "".join(("SparrowDistant - Timeout", str(toLaunch), " instances not launched in time"))
        
        return ips
     
    def terminateAll(self):   
        i = 0
        maxInst = len(self.instances)       
        for instance in self.instances:
            instance.terminate()
    
    def __init__(self):
        self.EC2 = boto.ec2.connect_to_region("us-west-2", aws_access_key_id= AWSAccessKeyId, aws_secret_access_key=AWSSecretKey)
        self.instances = []

#if __name__ == "__main__":
    #distant = SparrowDistant()
    
    #distant.launchInstances(3, 30)
    #distant.terminateAll()