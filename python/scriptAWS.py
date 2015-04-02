import boto, boto.ec2, time
from AWSconf import*

#boto.set_stream_logger('boto')	

number = 3

EC2 = boto.ec2.connect_to_region("us-west-2", aws_access_key_id= AWSAccessKeyId, aws_secret_access_key=AWSSecretKey)

reservation = EC2.run_instances(image_id= AMI, instance_type = 't2.micro', key_name= sshKey, min_count = number, max_count = number)

launched = 0
startTime = time.clock()

print len(reservation.instances)

while launched < number and (time.clock() - startTime) < 300000:
    print str(launched) + " instances running"
    time.sleep(10)
    launched = 0
    for i in reservation.instances:
        update = i.update()
        if update == "running":
            launched += 1
        
            
            
          
print "Terminated" + str(launched) + "launched instances"


#get ip address
#reservation = conn.run_instances(...)

#instance = reservation.instances[0]

#while instance.update() != "running":
#    time.sleep(5)  # Run this in a green thread, ideally

#print instance.ip_address
	
