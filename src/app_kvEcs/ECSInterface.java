package app_kvEcs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;










import app_kvClient.KVClient;
import app_kvServer.cache_strategies.Strategy;

import com.sun.corba.se.spi.orbutil.fsm.Input;

import client.KVStore;

public class ECSInterface {

	static String command;
	public enum Command {
		INIT, START, STOP, SHUTDOWN, HELP, QUIT, ADDNODE, DELETENODE
	}

	private Logger logger = Logger.getLogger(getClass().getSimpleName());
	private static final String PROMPT = "ECSClient> ";
	private BufferedReader stdin;
	private ECSClient ecsClient = new ECSClient();
	private boolean stop = false;

	public void run() {
		while(!stop) {	        	

			System.out.println("\n-------------------Please select one of the commands-------------------------------------");

			System.out.println("\nINIT, START, ADDNODE, DELETENODE, STOP, SHUTDOWN, HELP, QUIT");

			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print("\n" + PROMPT);

			try {
				String[] input = stdin.readLine().split(" ");
				Command cmd = Command.valueOf(input[0].toUpperCase());
				handleCommand(cmd, input);
			} catch (IllegalArgumentException e) {

				printError("Do not recognise "
						+ "the input, pl. try again");

				continue;
			} catch (IOException e) {
				printError("Did not get the input, "
						+ "pl. try again");

				continue;
			}
		}
	}


	private void printError(String error){
		System.out.println("\n" + PROMPT + "Error! " +  error);
	}

	private void handleCommand(Command cmd, String[] input) {
		try {
			switch (cmd) {

			case INIT:
				if(input.length != 4) {
					System.out.println("Error! Invalid number of arguments!");
					System.out.println("Usage: init <numberOfNodes> <cacheSize> <cacheStrategy> !");
				} else {
					int nodesNumber = Integer.valueOf(input[1]);
					int cacheSize = Integer.valueOf(input[2]);
					Strategy strategy = Strategy.valueOf(input[3]);
					if (cacheSize < 0 || nodesNumber < 1) {
						printError("Cache size must be greater than -1 and node"
								+ "number must be greater than 0.\nPlease try again.");
						break;
					}
					ecsClient.initService(nodesNumber, cacheSize, strategy);
				}
				break;

			case START:
				ecsClient.start();
				break;

			case STOP:
				stop = true;
				ecsClient.stop();
				break;

			case SHUTDOWN:
				stop = true;
				ecsClient.shutdown();
				break;

			case HELP:
				printHelp();
				break;

			case ADDNODE:
				if(input.length != 3) {
					System.out.println("Error! Invalid number of arguments!");
					System.out.println("Usage: addnode <cacheSize> <cacheStrategy> !");
				} else {
					int sizeCache = Integer.valueOf(input[1]);
					Strategy strat = Strategy.valueOf(input[2]);
					if (sizeCache < 0) {
						printError("Cache size must be greater than -1."
								+ "\nPlease try again.");
						break;
					}
					ecsClient.addNode(sizeCache, strat);
				}
				break;

			case DELETENODE:
				ecsClient.removeNode();
				break;

			case QUIT:	
				stop = true;
				ecsClient.shutdown();
				System.exit(0);
				break;

			default:
			}

		} catch (IOException e) {
			stop = true;
			printError("CLI does not respond - Application terminated ");
		} catch (NumberFormatException e) {
			printError("Numerical argument could not be parsed; please try again.");
		} catch (NoSuchAlgorithmException e) {
			stop = true;
			printError("A fatal error occurred - Application terminated");
			logger.fatal("Hashing algorithm does not exist.");
		}
	}


	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("INIT <Amount of nodes> <Cache size> <Displacement strategy>");
		sb.append("\t Initializes 'Amount of nodes' servers\n");
		sb.append("\t Displacement strategy : LRU | LFU | FIFO\n");
		sb.append(PROMPT).append("START");
		sb.append("\t\t Starts everything \n");
		sb.append(PROMPT).append("STOP");
		sb.append("\t\t Stops .. \n");
		sb.append(PROMPT).append("SHUTDOWN");
		sb.append("\t\t\t Shutdowns \n");

		sb.append(PROMPT).append("ADDNODE");
		sb.append("\t\t\t Add node \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

}

