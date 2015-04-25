package edu.berkeley.sparrow.examples;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;



public class FrontendMessageProcessing implements Runnable{

	private ConcurrentLinkedQueue<TimeMessage> fifo = new ConcurrentLinkedQueue<FrontendMessageProcessing.TimeMessage>();
	private boolean run = true;
	//private long[] endTimes;
	private Logger LOG;
	private BufferedWriter bw;
	private long startTime;
	private ServerSocket serverSocket;
	private Thread[] receptionThreads = new Thread[124];
	private String messageSeparator = ";";
	private Pattern pattern1 = Pattern.compile(messageSeparator);
	private String idEndTimeSeparator = ":";
	private Pattern pattern2 = Pattern.compile(idEndTimeSeparator);
	private ArrayList<Integer> taskIds = new ArrayList<Integer>();
	public long lastReceptionTime;
	private int numberOfTasks;
	
	public FrontendMessageProcessing(//int numberOfTasks,
			Logger log, ServerSocket serverSocket, BufferedWriter bw, long startTime, int numberOfTasks) {
		//this.endTimes = new long[numberOfTasks];
		this.LOG = log;
		this.serverSocket = serverSocket;
		this.bw = bw;
		this.startTime = startTime;
		this.numberOfTasks = numberOfTasks;
	}


	private class TimeMessage{
		private final long receptionTime;
		private final String message;

		public TimeMessage(long time, String message){
			this.receptionTime = time;
			this.message = message;
		}

	}

	private class ConnectionHandler implements Runnable{
		private int index = 0;
		@Override
		public void run() {
			while(run){
				LOG.debug("Connection Handler - waiting for connection");
				try {
					Socket socket = serverSocket.accept();
					MessageHandler messageHandler = new MessageHandler(socket);
					receptionThreads[index] = new Thread(messageHandler);
					receptionThreads[index].start();	
					LOG.debug("Connection Handler - connection made");
					index ++;
				} catch (SocketException e){
					LOG.debug("Connection Handler - socket exception => exiting");
					break;
				} catch (IOException e) {
					e.printStackTrace();
					break;
				}

			}
			LOG.debug("Connection Handler - exit complete");

		}
	}

	private class MessageHandler implements Runnable{
		private Socket receptionSocket;
		private MessageHandler(Socket socket){
			this.receptionSocket = socket;
		}
		@Override
		public void run() {
			DataInputStream in;
			LOG.debug("Message Handler - start");
			try {
				in = new DataInputStream(receptionSocket.getInputStream());
				while(run){
					if (in.available() > 0){
						LOG.debug("Message Handler - available > 0");
						String tasksCompletedBatch = in.readUTF();
						LOG.debug("RECEPTION MESSAGE : " + tasksCompletedBatch + " time " + System.currentTimeMillis());
						long receptionTime = System.currentTimeMillis();
						addMessage(receptionTime, tasksCompletedBatch);
					}else{
						//LOG.debug("Message Handler - available = 0");
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			LOG.debug("Message Handler - exit complete");
		}

	}

	public void addMessage(long time, String message){
		LOG.debug("FeMessageProcessing - message added in Q");
		TimeMessage timeMessage = new TimeMessage(time, message);
		fifo.add(timeMessage);
	}

	public void stop(){
		this.run = false;
	}

	@Override
	public void run() {
		//long lastBatchRecpTime;


		LOG.debug("FeMessageProcessing - started");
		ConnectionHandler connectionHandler = new ConnectionHandler();
		Thread connectionHandlerTh = new Thread(connectionHandler);
		connectionHandlerTh.start();

		while(run){
			if (fifo.isEmpty()){
				//TODO make sleep time a parameter -conf file-
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}else{
				//XXX
				int idx = 0;
				LOG.debug("FeMessageProcessing - reception");
				//lastBatchRecpTime = 0;
				TimeMessage timeMessage = fifo.poll();
				lastReceptionTime = timeMessage.receptionTime;
				LOG.debug("receptTime="+ timeMessage.receptionTime + " message="+ timeMessage.message);
				// 2 splitting. 1 to get (ID, delay) messages, then extract Id & delay
				String[] messageSplit = pattern1.split(timeMessage.message);
				//				LOG.debug("messagesplit1=" + messageSplit);
				long[] endTimes = new long[messageSplit.length]; // XXX redefinition of endTimes 
				for(String unitMessage : messageSplit){
					String[] info = pattern2.split(unitMessage);
					//					LOG.debug("messagesplit2=" + info);
					if(info.length != 2){
						LOG.error("FeMessageProcessing - Wrong message format, too much information in a single message");
					}else{
						Integer taskId = Integer.parseInt(info[0]);
						Long delay = Long.parseLong(info[1]);

						if(taskId == null || delay == null){
							LOG.error("FeMessageProcessing - Message format received not int:long");

						}else if(taskId == 0){ // reception of the batching delay. All tasks in the batch are subtracted the duration of the batching
							//lastBatchRecpTime = delay;
							for(int i = 0; i< endTimes.length - 1; i++){
								endTimes[i] = endTimes[i] - delay;
								LOG.debug("Sleep "+ taskIds.get(i) + " "+ (endTimes[i] - startTime) + "ms");
								try {
									bw.write(taskIds.get(i) + "," + "Full time" + "," + (endTimes[i] - startTime) + "," + "," + Thread.currentThread().getId() + "\n");
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								BFrontend.taskCompleted();
							}
							taskIds.clear(); //clear list for next batch

						}else if(taskId <= numberOfTasks){
							taskIds.add(taskId-1);
							endTimes[idx] = timeMessage.receptionTime + delay;  // + lastBatchRecpTime in case 2 batchs are in a single message
							
							idx ++;

						}else{
							LOG.error("FeMessageProcessing - Received incorrect task Id");
						}
					}
				}
				try {
					bw.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		LOG.debug("FeMessageProcessing - exiting");
		int idx = 0; 
		while(idx <= receptionThreads.length && receptionThreads[idx] != null ){
			try {
				receptionThreads[idx].join();
				idx++;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		try {
			serverSocket.close();
		} catch (IOException e) {
			LOG.error("FeMessageProcessing - Error while closing serversocket");
			e.printStackTrace();
		}
		try {
			connectionHandlerTh.join();
		} catch (InterruptedException e) {
			LOG.error("FeMessageProcessing - Error while joining connectionHandlerTh");
			e.printStackTrace();
		}
		LOG.debug("FeMessageProcessing - exit complete");
	}

//	public long[] getEndTimes() {
//		return endTimes;
//	}

}
