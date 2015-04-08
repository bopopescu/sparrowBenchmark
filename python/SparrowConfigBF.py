#Script to create multiple config files for sparrow frontend and backend
#the config files are stored in pwd/Config 
## File example ##

#BFrontend
#experiment_s = 
#number_tasks = 
#task_duration_millis = 

#BBackend
#app_client_ip = 
#batching_delay = 
#worker_threads = 

import argparse, os

pwd = "/home/ubuntu/sparrow"
#BFrontend
experiment_s_L = [60] 
number_tasks_L = [30]
task_duration_millis_L = [0] 

#BBackend
app_client_ip = ["172.31.22.75"]
batching_delay_L = []
worker_threads_L = [2]

#determines next combination of options
def next(optionIdxs, maxIdxs):
	i = len(optionIdxs) - 1
	run = True
	
	while(run and i >= 0):
		if maxIdxs[i] != -1:
			if optionIdxs[i] < maxIdxs[i]:
				optionIdxs[i] += 1
				run = False
			else:
				optionIdxs[i] = 0
		i -= 1
	

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description = "Creates config files for front/backend", prog = "SparrowConfigBF.py")
	parser.add_argument("-sf", "--startfrontend", type = int, default = 1)
	parser.add_argument("-sb", "--startbackend", type = int, default = 1)

	args = parser.parse_args() 

	os.chdir(pwd)

	#Frontend configs
	fileIdx = args.startfrontend
	maxIdxs = [len(experiment_s_L), len(number_tasks_L), len(task_duration_millis_L)]
	maxIdxs = [x - 1 for x in maxIdxs] 
	optionIdxs = maxIdxs[:]
	run = True

	while run:    
		next(optionIdxs, maxIdxs)
		fd = open('Conf/conf.Frontend'+str(fileIdx), 'w')
		if experiment_s_L:
			fd.write("experiment_s = "+ str(experiment_s_L[optionIdxs[0]]) +"\n")
		if number_tasks_L:
			fd.write("number_tasks = "+ str(number_tasks_L[optionIdxs[1]]) +"\n")
		if task_duration_millis_L:
			fd.write("task_duration_millis = "+ str(task_duration_millis_L[optionIdxs[2]]) +"\n")
		fd.close()
		fileIdx += 1

		if optionIdxs == maxIdxs:
			run = False
   	
   	print str(fileIdx - args.startfrontend) + " Frontend config files created"
   	
    #Backend configs
	fileIdx = args.startbackend
	maxIdxs = [len(app_client_ip), len(batching_delay_L), len(worker_threads_L)]
	maxIdxs = [x - 1 for x in maxIdxs] 
	optionIdxs = maxIdxs[:]
	run = True

	while run:    
		next(optionIdxs, maxIdxs)
		fd = open('Conf/conf.Backend'+str(fileIdx), 'w')
		if app_client_ip:
			fd.write("app_client_ip = "+ app_client_ip[optionIdxs[0]] +"\n")
		else:
			print "app_client_ip is mandatory, please define it"
		if batching_delay_L:
			fd.write("batching_delay = "+ str(batching_delay_L[optionIdxs[1]]) +"\n")
		if worker_threads_L:
			fd.write("worker_threads = "+ str(worker_threads_L[optionIdxs[2]]) +"\n")
		fd.close()
		fileIdx += 1

		if optionIdxs == maxIdxs:
			run = False
    
	print str(fileIdx - args.startbackend) + " Backend config files created"
