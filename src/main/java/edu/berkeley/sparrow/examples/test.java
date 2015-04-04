package edu.berkeley.sparrow.examples;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.thrift.TException;

public class test {
	
	public static void main(String[] args) throws IOException, TException {
		Level l = Level.toLevel("off", Level.DEBUG);
		System.out.println(l);

	}
}
