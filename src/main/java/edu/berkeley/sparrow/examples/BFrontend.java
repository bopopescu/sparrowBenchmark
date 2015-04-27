/**
 * 
 */
package edu.berkeley.sparrow.examples;

/**
 * @author Thomas Dubucq
 *
 */

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.api.SparrowFrontendClient;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Simple frontend that runs jobs composed of sleep tasks.
 */
public class BFrontend implements FrontendService.Iface {
	/** Amount of time to launch tasks for. */
	public static final String EXPERIMENT_S = "experiment_s";
	public static final int DEFAULT_EXPERIMENT_S = 300;

	/** Number of tasks */
	public static final String NUMBER_TASKS = "number_tasks";
	public static final int DEFAULT_NUMBER_TASKS = 10;

	/** Duration of one task, in milliseconds */
	public static final String TASK_DURATION_MILLIS = "task_duration_millis";
	public static final int DEFAULT_TASK_DURATION_MILLIS = 0;

	/** Host and port where scheduler is running. */
	public static final String SCHEDULER_HOST = "scheduler_host";
	public static final String DEFAULT_SCHEDULER_HOST = "localhost";
	public static final String SCHEDULER_PORT = "scheduler_port";

	public static final String APPLICATION_ID = "Bsleep";

	private static final Logger LOG = Logger.getLogger(BFrontend.class);
	private static final String DEFAULT_LOG_LEVEL = "debug";
	private static final String LOG_LEVEL = "log_level";

	private static final TUserGroupInfo USER = new TUserGroupInfo();

	private static final String PORT = "port";
	private static final int DEFAULT_PORT = 25501;
	private SparrowFrontendClient client;

	private static int tasksCompleted = 0;

	public static void taskCompleted(){
		tasksCompleted++;
	}


	/** A runnable which Spawns a new thread to launch a scheduling request. */
	private class JobLaunchRunnable implements Runnable {
		private int taskDurationMillis;
		private int NumberTasks;
		private long startTime;

		public JobLaunchRunnable(int NumberTasks, int taskDurationMillis) {
			this.NumberTasks = NumberTasks;
			this.taskDurationMillis = taskDurationMillis;
		}

		@Override
		public void run() {
			// Generate tasks in the format expected by Sparrow. First, pack task parameters.
			ByteBuffer message = ByteBuffer.allocate(4);
			message.putInt(taskDurationMillis);

			startTime = System.currentTimeMillis();
			List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
			int taskId = 1;
			int batch = 0;
			TTaskSpec spec = new TTaskSpec();
			while(taskId <= NumberTasks){
				spec.setTaskId(Integer.toString(taskId));
				spec.setMessage(message.array());
				tasks.add(spec);
				taskId++;
				batch++;
				if(batch >= 10000){
					try {
						client.submitJob(APPLICATION_ID, tasks, USER);
					} catch (TException e) {
						LOG.error("Scheduling request failed!", e);
					}
					spec = new TTaskSpec();
					batch = 0;
				}
			}

			try {
				client.submitJob(APPLICATION_ID, tasks, USER);
			} catch (TException e) {
				LOG.error("Scheduling request failed!", e);
			}
			long end = System.currentTimeMillis();
			LOG.debug("Scheduling request duration " + (end - startTime) + "ms");
		}

	}

	public void run(String[] args) {
		try {
			OptionParser parser = new OptionParser();
			parser.accepts("c", "configuration file").withRequiredArg().ofType(String.class);
			parser.accepts("help", "print help statement");
			OptionSet options = parser.parse(args);

			if (options.has("help")) {
				parser.printHelpOn(System.out);
				System.exit(-1);
			}

			// Logger configuration: log to the console
			BasicConfigurator.configure();

			Configuration conf = new PropertiesConfiguration();

			if (options.has("c")) {
				String configFile = (String) options.valueOf("c");
				conf = new PropertiesConfiguration(configFile);
			}

			int experimentDurationS = conf.getInt(EXPERIMENT_S, DEFAULT_EXPERIMENT_S);
			int numberTasks = conf.getInt(NUMBER_TASKS, DEFAULT_NUMBER_TASKS);
			int taskDurationMillis = conf.getInt(TASK_DURATION_MILLIS, DEFAULT_TASK_DURATION_MILLIS);
			LOG.setLevel(Level.toLevel(conf.getString(LOG_LEVEL, DEFAULT_LOG_LEVEL)));
			int schedulerPort = conf.getInt(SCHEDULER_PORT,
					SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);
			String schedulerHost = conf.getString(SCHEDULER_HOST, DEFAULT_SCHEDULER_HOST);
			int port = conf.getInt(PORT,DEFAULT_PORT);
			LOG.debug("BFrontend - conf\n experiment_s = "+ experimentDurationS + "s numberTasks = " + numberTasks
					+ " taskDuration = " + taskDurationMillis + "ms");
			ServerSocket serverSocket = new ServerSocket(port);

			client = new SparrowFrontendClient();
			client.initialize(new InetSocketAddress(schedulerHost, schedulerPort), APPLICATION_ID, this);
			FileWriter fw = new FileWriter("Results.txt");
			BufferedWriter bw = new BufferedWriter(fw);
			LOG.debug("Client initilized");

			long startTime = System.currentTimeMillis();
			FrontendMessageProcessing messageProcessing = new FrontendMessageProcessing(//numberTasks, 
					LOG, serverSocket, bw, startTime, numberTasks);
			Thread messageProcessingTh = new Thread(messageProcessing);
			messageProcessingTh.start();

			LOG.debug("Message processing initilized");

			JobLaunchRunnable runnable = new JobLaunchRunnable(numberTasks, taskDurationMillis);
			Thread jobLaunch = new Thread(runnable);
			jobLaunch.start();

			LOG.debug("sleeping");
			long maxEndTime = startTime + experimentDurationS * 1000;
			while (tasksCompleted < numberTasks && System.currentTimeMillis() < maxEndTime ) {
				LOG.debug("Tasks completed = "+ tasksCompleted);
				Thread.sleep(500);
			}
			long endTime = messageProcessing.lastReceptionTime;
			serverSocket.close();
			messageProcessing.stop();
			messageProcessingTh.join();
			jobLaunch.join();
			
			LOG.debug("task completed " + tasksCompleted + " start " + startTime + " end " + endTime + " maxend " + maxEndTime);

			String ip = InetAddress.getLocalHost().toString();
			if(endTime < maxEndTime || tasksCompleted == numberTasks){
				long expTime = endTime - startTime;
				LOG.debug("Tasks completed = "+ tasksCompleted);
				LOG.debug("Experiment ended  in " + (expTime) + "ms");
				bw.write("0," + "Experiment full time" + "," + expTime + "," + ip + "," + Thread.currentThread().getId() + "\n");
			}else{
				LOG.debug("Experiment ended - Timeout");
			}

			//long[] endTimes = messageProcessing.getEndTimes();

			//XXX
			/*
			long[] endTimes = messageProcessing.getEndTimes();
			for(int i = 0; i < endTimes.length; i++ ){
				LOG.debug("Sleep "+ i + " "+ (endTimes[i] - startTime) + "ms");
				bw.write((i+1) + "," + "Full time" + "," + (endTimes[i] - startTime) + "," + ip + "," + Thread.currentThread().getId() + "\n");
				
			}*/
			bw.flush();
			bw.close();
			fw.flush();
			fw.close();
			//XXX
			client.close();

		}
		catch (Exception e) {
			LOG.error("Fatal exception", e);
		}
		
		FileWriter fw;
		try {
			fw = new FileWriter("Finish.txt");
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write("Experience finished\n");
			bw.flush();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.debug("BFronted exit complete");
		System.out.println("BFronted exit complete");
		
		return;

	}


	@Override
	public void frontendMessage(TFullTaskId taskId, int status, ByteBuffer message)
			throws TException {
		// We don't use messages here, so just log it.
		LOG.debug("Got unexpected message: " + Serialization.getByteBufferContents(message));
	}

	public static void main(String[] args) {
		new BFrontend().run(args);
	}
}
