package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVSocketListener;
import client.KVStore;
import common.messages.TextMessage;

public class KVClient implements KVSocketListener {

	static String command;

	private Logger logger = Logger.getLogger(getClass().getSimpleName());
	private static final String PROMPT = "KVClient> ";
	private BufferedReader stdin;
	private KVStore kvStore = new KVStore("127.0.0.1", 50000);
	private boolean stop = false;

	private String serverAddress;
	private int serverPort;

	public void run() {	        	

		System.out.println("\n-------------------Please select one of the commands-------------------------------------");

		System.out.println("\nPUT, GET, LOGLEVEL, HELP, QUIT");

		while(!stop) {

			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);

			try {
				String command = stdin.readLine();
				String[] tokens = command.split("\\s+");

				if(tokens[0].equals("quit")) {
					stop = true;
					disconnect();
					System.out.println(PROMPT + "Application exit!");

				} else if (tokens[0].equals("put")) {
					if(tokens.length == 3) {
						try {
							System.out.println(kvStore.put(tokens[1], tokens[2]));
						} catch (NoSuchAlgorithmException e) {
							printError("A fatal error occurred!");
							disconnect();
							stop = true;
						} catch (IOException e) {
							printError("Unable to send message!");
							disconnect();
						} catch (InterruptedException e) {
							printError("Unable to receive answer!");
							disconnect();
						}
					} else {
						printError("Invalid number of parameters!");
					}

				} else if (tokens[0].equals("get")) {
					if(tokens.length == 2) {
						try {
							System.out.println(kvStore.get(tokens[1]));
						} catch (NoSuchAlgorithmException e) {
							printError("A fatal error occurred!");
							disconnect();
							stop = true;
						} catch (IOException e) {
							printError("Unable to send message!");
							disconnect();
						} catch (InterruptedException e) {
							printError("Unable to receive answer!");
							disconnect();
						}
					} else {
						printError("Invalid number of parameters!");
					}

				} else if(tokens[0].equalsIgnoreCase("logLevel")) {
					if (tokens.length == 1) {
						System.out.println("\t" +
								"Log level currently set to level " + logger.getLevel());
					} else if(tokens.length == 2) {
						String level = setLevel(tokens[1]);
						System.out.println("\t" +
								"Log level changed to level " + level);
					} else {
						printError("Invalid number of parameters!");
					}

				} else if(tokens[0].equals("help")) {
					printHelp();
				} else {
					printError("Unknown command");
					printHelp();

					continue;

				}

			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}   
	}


	private void disconnect() {
		if(kvStore != null) {
			kvStore.disconnect();
		}
	}

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t inserts or overwrites record in kvStore \n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t reads record from kvStore \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");

		sb.append(PROMPT).append("logLevel <level>");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

	private void printPossibleLogLevels() {
		System.out.println(PROMPT
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {
		Level wantedLevel = Level.toLevel(levelString);
		logger.setLevel(wantedLevel);
		return wantedLevel.toString();
		//		if(levelString.equals(Level.ALL.toString())) {
		//			logger.setLevel(Level.ALL);
		//			return Level.ALL.toString();
		//		} else if(levelString.equals(Level.DEBUG.toString())) {
		//			logger.setLevel(Level.DEBUG);
		//			return Level.DEBUG.toString();
		//		} else if(levelString.equals(Level.INFO.toString())) {
		//			logger.setLevel(Level.INFO);
		//			return Level.INFO.toString();
		//		} else if(levelString.equals(Level.WARN.toString())) {
		//			logger.setLevel(Level.WARN);
		//			return Level.WARN.toString();
		//		} else if(levelString.equals(Level.ERROR.toString())) {
		//			logger.setLevel(Level.ERROR);
		//			return Level.ERROR.toString();
		//		} else if(levelString.equals(Level.FATAL.toString())) {
		//			logger.setLevel(Level.FATAL);
		//			return Level.FATAL.toString();
		//		} else if(levelString.equals(Level.OFF.toString())) {
		//			logger.setLevel(Level.OFF);
		//			return Level.OFF.toString();
		//		} else {
		//			return LogSetup.UNKNOWN_LEVEL;
		//		}
	}

	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " 
					+ serverAddress + " / " + serverPort);

		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ serverAddress + " / " + serverPort);
			System.out.print(PROMPT);
		}

	}

	@Override
	public void handleNewMessage(TextMessage msg) {
		System.out.println("Got new message in client");

	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

	/**
	 * Main entry point for the kvStore application. 
	 * @param args contains the port number at args[0].
	 * @throws NoSuchAlgorithmException 
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/client.log", Level.ALL);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

}