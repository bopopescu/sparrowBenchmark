package edu.berkeley.sparrow.examples;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SynchronizedWrite implements Runnable{
	private FileWriter fw;
	private BufferedWriter bw;
	
	public synchronized void write(String taskID, long length, String state, String ip){
			try {
				bw.write(taskID + "," + state + "," + length + "," + ip + "," + Thread.currentThread().getId() + "\n");
			} catch (IOException e) {
				System.out.println("SynchornizedWrite - write() IOException");
				e.printStackTrace();
			}
	}
	
	public SynchronizedWrite(String file) {
		try {
			this.fw = new FileWriter(file);
			this.bw = new BufferedWriter(fw);
		} catch (IOException e) {
			System.out.println("SynchornizedWrite - IOException at initialization");
			e.printStackTrace();
		}
		
	}

	@Override
	public void run() {
		while(true){
			try {
				Thread.sleep(100);
				this.bw.flush();
			} catch (InterruptedException e) {
				System.out.println("SynchornizedWrite - run() Interrupted");
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("SynchornizedWrite - run() IOException");
				e.printStackTrace();
			}
		}
	}
}
