package app_kvEcs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.NoSuchElementException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

















import app_kvApi.KVStore;
import app_kvClient.KVClient;
import app_kvServer.cache_strategies.Strategy;

import com.sun.corba.se.spi.orbutil.fsm.Input;

public class ECSInterface {

	static String command;
	public enum Command {
		INIT, START, STOP, SHUTDOWN, HELP, QUIT, ADDNODE, REMOVENODE
	}

	private static final Logger logger = Logger.getLogger(ECSInterface.class);
	private static final String PROMPT = "ECSClient> ";
	private BufferedReader stdin;
	private final ECSClient ecsClient;
	private boolean appStopped = false;
	private boolean initialized = false;

	public ECSInterface() {
		this.ecsClient = new ECSClient("./src/app_kvEcs/ecs.config.txt");
	}

	public ECSInterface(String configLocation) {
		this.ecsClient = new ECSClient(configLocation);
	}

	public void run() {

		System.out.println("\n-------------------Please select one of the commands-------------------------------------");
		System.out.println("\nINIT, START, ADDNODE, REMOVENODE, STOP, SHUTDOWN, HELP, QUIT");

		while(!appStopped) {	        	

			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print("\n" + PROMPT);

			try {
				String input = stdin.readLine();
				handleCommand(input);
			} catch (IOException e) {
				printError("Did not get the input, "
						+ "pl. try again");
			}
		}
		System.out.println("Application terminated !");
	}


	public void handleCommand(String line) {
		try{
			String[] input = line.split(" ");
			Command cmd = Command.valueOf(input[0].toUpperCase());
			handleCommandBis(cmd, input);
		} catch (IllegalArgumentException e) {
			printError("Do not recognise "
					+ "the input, pl. try again");
		}
	}

	private void handleCommandBis(Command cmd, String[] input) {
		try {
			switch (cmd) {

			case INIT:
				if (initialized) {
					System.out.println("\t System already initialized."
							+ "\n\t Please use ADDNODE, or SHUTDOWN and INIT again.");
				}
				else if (input.length != 4) {
					printError("Invalid number of arguments! "
							+ "\n\t Usage: init <numberOfNodes> <cacheSize> <cacheStrategy> !");
				} else {
					int nodesNumber = Integer.valueOf(input[1]);
					int cacheSize = Integer.valueOf(input[2]);
					Strategy strategy = Strategy.valueOf(input[3]);
					if (cacheSize < 0 || nodesNumber < 1) {
						printError("Cache size must be greater than -1 and node"
								+ "number must be greater than 0.\nPlease try again.");
						break;
					}
					int initNodes = ecsClient.initService(nodesNumber, cacheSize, strategy);
					System.out.println("\t System initialized with " + initNodes + " nodes.");
					initialized = true;
				}
				break;

			case START:
				if (initialized) {
					ecsClient.start();
				} else {
					System.out.println("\t System must be initialized."
							+ "\n\t Please use INIT command.");
				}
				break;

			case STOP:
				ecsClient.stop();
				break;

			case SHUTDOWN:
				ecsClient.shutdown();
				initialized = false;
				break;

			case HELP:
				printHelp();
				break;

			case ADDNODE:
				if (!initialized) {
					System.out.println("\t System must be initialized."
							+ "\n\t Please use INIT command.");
					break;
				}

				if(input.length != 3) {
					printError("Invalid number of arguments! "
							+ "\n\t Usage: ADDNODE <cacheSize> <cacheStrategy> !");
				} else {
					int sizeCache = Integer.valueOf(input[1]);
					Strategy strat = Strategy.valueOf(input[2]);
					if (sizeCache < 0) {
						printError("Cache size must be greater than -1."
								+ "\n\t Please try again.");
						break;
					}
					try {
						int nb = ecsClient.addNode(sizeCache, strat);
						System.out.println("\t Node added successfully."
								+ "\n\t There are " + nb + " nodes in the system.");
					} catch (NoSuchElementException e) {
						System.out.println("\t There is no more node to be added."
								+ "\n\t Please use another command.");
					} catch (IOException e) {
						System.out.println("\t Adding failed."
								+ "\n\t Please try again.");
					}
				}
				break;

			case REMOVENODE:
				if (!initialized) {
					System.out.println("\t System must be initialized."
							+ "\n\t Please use INIT command.");
					break;
				}
				int remainingNodes = 0;
				try {
					remainingNodes = ecsClient.removeNode();
					System.out.println("\t Node removed successfully."
							+ "\n\t There are " + remainingNodes + " remaining nodes"
									+ " in the system.");
				} catch (IllegalArgumentException e) {
					System.out.println("\t There is only one node currently."
							+ "\n\t If you delete it your data will be lost."
							+ "\n\t Please use SHUTDOWN if you really want to delete it.");
				}
					break;

			case QUIT:	
				appStopped = true;
				ecsClient.shutdown();
				logger.info("Exiting");
				break;

			default:
			}

		} catch (NumberFormatException e) {
			printError("Numerical argument could not be parsed; please try again.");
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			appStopped = true;
			printError("A fatal error occurred - Application terminated");
			logger.fatal("Hashing algorithm does not exist or used encoding is not supported.");
		} catch (IOException e) {
			appStopped = true;
			printError("CLI does not respond - Application terminated ");
		}
	}


	private void printError(String error){
		System.out.println("\t" + "Error! " +  error);
	}


	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append("\t").append("ECS CLIENT HELP (Usage):\n\n");
		sb.append("\t");
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n\n");
		sb.append("\t").append("INIT <Amount of nodes> <Cache size> <Displacement strategy>\n");
		sb.append("\t\t\t Initializes 'Amount of nodes' servers\n");
		sb.append("\t\t\t Displacement strategy : LRU | LFU | FIFO\n");
		sb.append("\t").append("START \n");
		sb.append("\t\t Starts all nodes \n");
		sb.append("\t").append("ADDNODE <Cache size> <Displacement strategy> \n");
		sb.append("\t\t\t Add next node \n");
		sb.append("\t\t\t Displacement strategy : LRU | LFU | FIFO\n");
		sb.append("\t").append("REMOVENODE \n");
		sb.append("\t\t\t Remove last added node \n");
		sb.append("\t").append("STOP \n");
		sb.append("\t\t\t Stops all nodes \n");
		sb.append("\t").append("SHUTDOWN \n");
		sb.append("\t\t\t Shutdowns all nodes \n");
		//		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		sb.append("\t").append("QUIT \n");
		sb.append("\t\t\t Exits the program");
		System.out.println(sb.toString());
	}

}

