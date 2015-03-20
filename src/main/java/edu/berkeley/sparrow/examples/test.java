package edu.berkeley.sparrow.examples;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.thrift.TException;

public class test {
	private static final String APP_CLIENT_IP = "app_client_ip";
	private static InetAddress appClientAdress;
	private static int appClientPortNumber;
	private static final String APP_CLIENT_PORT_NUMBER = "app_client_port_number";
	private static final int DEFAULT_APP_CLIENT_PORT_NUMBER = 25501;
	
	/*private void launchBatching() {
		try {

			Socket toClient = new Socket(appClientAdress, appClientPortNumber);

			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} 

		catch (IOException e) {
			e.printStackTrace();
		}
		
	}*/
	
	public static void main(String[] args) throws IOException, TException {
		OptionParser parser = new OptionParser();
		parser.accepts("c", "configuration file").
		withRequiredArg().ofType(String.class);
		parser.accepts("help", "print help statement");
		OptionSet options = parser.parse(args);

		if (options.has("help")) {
			parser.printHelpOn(System.out);
			System.exit(-1);
		}


		Configuration conf = new PropertiesConfiguration();

		if (options.has("c")) {
			String configFile = (String) options.valueOf("c");
			try {
				conf = new PropertiesConfiguration(configFile);
			} catch (ConfigurationException e) {}
		}
		// Start backend server
		System.out.println(conf.getString(APP_CLIENT_IP));
		appClientAdress = InetAddress.getByName(conf.getString(APP_CLIENT_IP));
		appClientPortNumber = conf.getInt(APP_CLIENT_PORT_NUMBER, DEFAULT_APP_CLIENT_PORT_NUMBER);


	}
}
