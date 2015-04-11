package edu.berkeley.sparrow.examples;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

public class Batching implements Runnable{

	private long batchingDelay;
	private static ConcurrentLinkedQueue<Info> fifo = new ConcurrentLinkedQueue<Info>();
	private boolean run = true;
	private Socket toClient;
	private Logger LOG;

	public Batching(long delay, Socket socket, Logger log){
		batchingDelay = delay;
		toClient = socket;
		LOG = log;
	}

	private static class Info{
		private long time;
		private int id;

		public Info (int id, long time){
			this.time =time;
			this.id = id;
		}
	}

	public static void add(int id, long time){
		Info info = new Info(id,time);
		fifo.add(info);
	}

	public void stop(){
		this.run = false;
	}

	@Override
	public void run() {
		long batchingStartTime;
		String message;
		Info info;
		LOG.debug("batching started");
		try {
			OutputStream  outToClient = toClient.getOutputStream();
			DataOutputStream out =	new DataOutputStream(outToClient);
			while(run){
				info = fifo.poll();
				if(info == null){
					try {
						Thread.sleep(batchingDelay/10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}else{
					LOG.debug("batching fifo pop");
					batchingStartTime = info.time;
					message = info.id + ":0;";
					while(System.currentTimeMillis()-batchingStartTime < batchingDelay ){
						info = fifo.poll();
						if(info == null){
							try {
								Thread.sleep(batchingDelay/10);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}else{
							message = message + info.id + ":" +(info.time - batchingStartTime) + ";";
						}
					}
					message = message + "0" + ":" +(System.currentTimeMillis() - batchingStartTime) + ";";
					LOG.debug("batching message:" + message);
					out.writeUTF(message);
					LOG.debug("batching message sent");
				}
			}
		} catch (IOException e) {
			LOG.error("Batching run - IOException");
			e.printStackTrace();
		}
	}
}
