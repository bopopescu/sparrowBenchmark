import boto, boto.ec2, time
from AWSconf import*

class SparrowDistant:


    def launchInstances(self, number, timeOut):
        reservation = self.EC2.run_instances(image_id= AMI, instance_type = instanceType, key_name= sshKey, min_count = number, max_count = number, security_group_ids=[securityGroup])
        notLaunched = range(number)
        ips = []
        privateIps = []
        toLaunch = number
        time.sleep(5)
        startTime = time.time()

        while toLaunch > 0 and (time.time() - startTime) < timeOut:
            print "".join(("SparrowDistant - still ", str(toLaunch), " instance(s) to launch")) 

            idx = notLaunched.pop(0)
            update = reservation.instances[idx].update()
            if update == "running":
                ips.append(reservation.instances[idx].ip_address)
                privateIps.append(reservation.instances[idx].private_ip_address)
                self.instances.append(reservation.instances[idx])
                toLaunch -= 1
            else:
                notLaunched.append(idx)
                time.sleep(5)
        
        if toLaunch == 0:
            print "".join(("SparrowDistant - ", str(number), " instance(s) launched in ", str(time.time() - startTime), " seconds"))
        else:
            print "".join(("SparrowDistant - Timeout", str(toLaunch), " instances not launched in time"))
        
        return (privateIps, ips)
     
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
