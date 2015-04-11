package edu.berkeley.sparrow.examples;

/*
 * Copyright 2013 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;

import edu.berkeley.sparrow.daemon.nodemonitor.NodeMonitorThrift;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.BackendService;
import edu.berkeley.sparrow.thrift.NodeMonitorService.Client;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;


/**
 * A prototype Sparrow backend that runs sleep tasks.
 */
public class BBackend implements BackendService.Iface {

	private static final String LISTEN_PORT = "listen_port";
	private static final int DEFAULT_LISTEN_PORT = 20101;

	private boolean batchingLaunched = false;
	private Thread batchingTh;
	private Batching batchingPr;
	private static long batchingDelay;
	private static final String BATCHING_DELAY = "batching_delay";
	private static final long DEFAULT_BATCHING_DELAY = 200;
	private static final String APP_CLIENT_PORT_NUMBER = "app_client_port_number";
	private static final int DEFAULT_APP_CLIENT_PORT_NUMBER = 25501;
	private static int appClientPortNumber;
	private static final String APP_CLIENT_IP = "app_client_ip";
	private static InetAddress appClientAdress;
	
	

	/**
	 * Each task is launched in its own thread from a thread pool with WORKER_THREADS threads,
	 * so this should be set equal to the maximum number of tasks that can be running on a worker.
	 */
	private static final int DEFAULT_WORKER_THREADS = 1;
	private static final String WORKER_THREADS = "worker_threads";
	private static final String APP_ID = "Bsleep";

	/** Configuration parameters to specify where the node monitor is running. */
	private static final String NODE_MONITOR_HOST = "node_monitor_host";
	private static final String DEFAULT_NODE_MONITOR_HOST = "localhost";
	private static String NODE_MONITOR_PORT = "node_monitor_port";

	private static Client client;

	private static final Logger LOG = Logger.getLogger(BBackend.class);
	private static final String DEFAULT_LOG_LEVEL = "debug";
	private static final String LOG_LEVEL = "log_level";
	
	private static ExecutorService executor;

	private static SynchronizedWrite resultLog;
	private static String ipAddress;
	
	/**
	 * Keeps track of finished tasks.
	 *
	 * A single thread pulls items off of this queue and uses
	 * the client to notify the node monitor that tasks have finished.
	 */
	private final BlockingQueue<TFullTaskId> finishedTasks = new LinkedBlockingQueue<TFullTaskId>();


	/**
	 * Thread that sends taskFinished() RPCs to the node monitor.
	 *
	 * We do this in a single thread so that we just need a single client to the node monitor
	 * and don't need to create a new client for each task.
	 */
	private class TasksFinishedRpcRunnable implements Runnable {
		@Override
		public void run() {
			while (true) {
				try {
					TFullTaskId task = finishedTasks.take();
					long endTime = System.currentTimeMillis();
					Batching.add(Integer.parseInt(task.taskId), endTime);
					client.tasksFinished(Lists.newArrayList(task));
				} catch (InterruptedException e) {
					LOG.error("Error taking a task from the queue: " + e.getMessage());
				} catch (TException e) {
					LOG.error("Error with tasksFinished() RPC:" + e.getMessage());
				}
			}
		}
	}


	/**
	 * Thread spawned for each task. It runs for a given amount of time (and adds
	 * its resources to the total resources for that time) then stops. It updates
	 * the NodeMonitor when it launches and again when it finishes.
	 */
	private class TaskRunnable implements Runnable {
		private int taskDurationMillis;
		private TFullTaskId taskId;

		public TaskRunnable(String requestId, TFullTaskId taskId, ByteBuffer message) {
			this.taskDurationMillis = message.getInt();
			this.taskId = taskId;
		}

		@Override
		public void run() {
			long startTime = System.currentTimeMillis();
			try {
				Thread.sleep(taskDurationMillis);
			} catch (InterruptedException e) {
				LOG.error("Interrupted while sleeping: " + e.getMessage());
			}
			long endTime = System.currentTimeMillis();
			LOG.debug("Task completed in " + (endTime - startTime) + "ms");
			resultLog.write(taskId.taskId, endTime - startTime, "running", ipAddress);
			finishedTasks.add(taskId);
		}
	}

	/**
	 * Initializes the backend by registering with the node monitor.
	 *
	 * Also starts a thread that handles finished tasks (by sending an RPC to the node monitor).
	 */
	public void initialize(int listenPort, String nodeMonitorHost, int nodeMonitorPort) {
		// Register server.
		try {
			client = TClients.createBlockingNmClient(nodeMonitorHost, nodeMonitorPort);
		} catch (IOException e) {
			LOG.debug("Error creating Thrift client: " + e.getMessage());
		}

		try {
			client.registerBackend(APP_ID, "localhost:" + listenPort);
			LOG.debug("Client successfully registered");
		} catch (TException e) {
			LOG.debug("Error while registering backend: " + e.getMessage());
		}

		new Thread(new TasksFinishedRpcRunnable()).start();
	}

	@Override
	public void launchTask(ByteBuffer message, TFullTaskId taskId,
			TUserGroupInfo user) throws TException {
		if(batchingLaunched == false){
			launchBatching();
			batchingLaunched = true;
		}
		LOG.info("Submitting task " + taskId.getTaskId() + " at " + System.currentTimeMillis());

		executor.submit(new TaskRunnable(
				taskId.requestId, taskId, message));
	}

	private void launchBatching() {
		try {

			Socket toClient = new Socket(appClientAdress, appClientPortNumber);
			batchingPr = new Batching(batchingDelay, toClient, LOG);
			batchingTh = new Thread(batchingPr);
			batchingTh.start();
			
		} catch (UnknownHostException e) {
			LOG.error("LaunchBatching - Unknown Host");
			e.printStackTrace();
		} 

		catch (IOException e) {
			LOG.error("LaunchBatching - IOException");
			e.printStackTrace();
		}
		
	}

	public static void main(String[] args) throws IOException, TException {
		ipAddress = InetAddress.getLocalHost().toString();
		OptionParser parser = new OptionParser();
		parser.accepts("c", "configuration file").
		withRequiredArg().ofType(String.class);
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
			try {
				conf = new PropertiesConfiguration(configFile);
			} catch (ConfigurationException e) {}
		}
		// Start backend server
		LOG.setLevel(Level.toLevel(conf.getString(LOG_LEVEL, DEFAULT_LOG_LEVEL)));
		LOG.debug("debug logging on");
		int listenPort = conf.getInt(LISTEN_PORT, DEFAULT_LISTEN_PORT);
		int nodeMonitorPort = conf.getInt(NODE_MONITOR_PORT, NodeMonitorThrift.DEFAULT_NM_THRIFT_PORT);
		batchingDelay = conf.getLong(BATCHING_DELAY, DEFAULT_BATCHING_DELAY);
		String nodeMonitorHost = conf.getString(NODE_MONITOR_HOST, DEFAULT_NODE_MONITOR_HOST);
		int workerThread = conf.getInt(WORKER_THREADS, DEFAULT_WORKER_THREADS);
		appClientAdress = InetAddress.getByName(conf.getString(APP_CLIENT_IP));
		appClientPortNumber = conf.getInt(APP_CLIENT_PORT_NUMBER, DEFAULT_APP_CLIENT_PORT_NUMBER);
		executor = Executors.newFixedThreadPool(workerThread);
		// Starting logging of results
		resultLog = new SynchronizedWrite("ResultsBackend.txt");
		Thread resultLogTh = new Thread(resultLog);
		resultLogTh.start();
		
		BBackend protoBackend = new BBackend();
		BackendService.Processor<BackendService.Iface> processor =
				new BackendService.Processor<BackendService.Iface>(protoBackend);

		TServers.launchSingleThreadThriftServer(listenPort, processor);
		protoBackend.initialize(listenPort, nodeMonitorHost, nodeMonitorPort);

	}
}



