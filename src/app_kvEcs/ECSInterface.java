package app_kvEcs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;



import client.KVStore;

public class ECSInterface {

	static String command;
	public enum Command {
		INIT, START, STOP, SHUTDOWN, HELP, QUIT, ADDNODE, DELETENODE
	}
	
	   private static Logger logger = Logger.getRootLogger();
	   private static final String PROMPT = "<KVClient> ";
	    private static final String DEFAULT_SERVER_ADDRESS = "localhost";
	    private static final int DEFAULT_SERVER_PORT = 50000;
	    private BufferedReader stdin;
	    private KVStore kvStore = null;
	    private boolean stop = false;
	    
	    private String serverAddress;
	    private int serverPort;
	    
	    public void run() {
	        while(!stop) {	        	
	        	
	        	System.out.println("\n-------------------Please select one of the commands-------------------------------------");
	            
	        	System.out.println("\nINIT, START, STOP, SHUTDOWN, HELP, QUIT");
	        	
	                    stdin = new BufferedReader(new InputStreamReader(System.in));
	                    System.out.print(PROMPT);
	        	
	                    try {
	                        String command = stdin.readLine();
	                        
	                        Command cmdLine = null;

	                		try {
	                				cmdLine = Command.valueOf(command.toUpperCase());
	                		} catch (IllegalArgumentException e) {
	                				
	                			System.out.println("\nDo not recognise "
	                					+ "the input, pl. try again");
	                		
	                			continue;
	                		}
	                        
	                        switch (cmdLine) {
	                        
	                        case INIT:
	                        	
	         
	                        case START:
	                        	

	                        case STOP:
	                        	
	                        
	                        case SHUTDOWN:

	                        
	                        case HELP:
	                        	
	                        	
	                        case QUIT:	
	                        	
	                        	
	                        case ADDNODE:
	                        	
	                        	
	                        case DELETENODE:	
	                        }
	                        
	                        
	                        this.handleCommandBis(cmdLine);
	                        
	                        // rather call handleCommandBis(cmdLine)
	                    } catch (IOException e) {
	                        stop = true;
	                        printError("CLI does not respond - Application terminated ");
	                    }
	                    }   
	        }

	    private void printError(String error){
	        System.out.println(PROMPT + "Error! " +  error);
	    }
	    
	    private void handleCommandBis(Command cmdLine) {
	        // decode the cmdLine like in handleCommand from ms2
	        // // call the function getServerForKey to know which server to connect
	        //// KVMessage answer = this.handleCommandWithServer(cmdLine, serverip, serverport);
	        // // if answer is success or eror... print it
	        // // if "not responsible": update metadata file and call handleCommandBis(cmdLine);
	        //
	    }
	    
	    private void printHelp() {
	        StringBuilder sb = new StringBuilder();
	        sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
	        sb.append(PROMPT);
	        sb.append("::::::::::::::::::::::::::::::::");
	        sb.append("::::::::::::::::::::::::::::::::\n");
	        sb.append(PROMPT).append("INIT <Amount of nodes>");
	        sb.append("\t initializes 'Amount of nodes' amount of servers\n");
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

