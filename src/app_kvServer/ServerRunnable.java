package app_kvServer;

import java.io.IOException;
import java.net.Socket;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVCommModule;

public class ServerRunnable implements Runnable {

	protected Socket clientSocket = null;
    protected String serverText   = null;
	private static final String PROMPT = "KVServer> ";
	private KVCommModule commModule;
	private boolean stop = true;
	private final CacheManager sharedCacheManager;

    public ServerRunnable(Socket clientSocket, String serverText, CacheManager cacheManager) {
        this.clientSocket = clientSocket;
        this.serverText   = serverText;
        this.sharedCacheManager = cacheManager;
    }
    
    public void run() {
    	stop = false;
    	try {
			commModule = new KVCommModule(clientSocket.getOutputStream(), clientSocket.getInputStream());
		} catch (IOException e1) {
			stop = true;
			printError("A connection error occurred - Application terminated " + e1);
		}
		
		while(!stop) {
			try {
				KVMessage requestFromClient = commModule.receiveKVMessage();
				System.out.println(PROMPT + "Requested from client : " + requestFromClient.getStatus() + 
						" " + requestFromClient.getKey() + " " + requestFromClient.getValue());
				
				KVMessage serverAnswer = handleCommand(requestFromClient);
				
				System.out.println(PROMPT + "Answer to client : " + serverAnswer.getStatus() + 
						" " + serverAnswer.getKey() + " " + serverAnswer.getValue());
				if (serverAnswer != null) {
					commModule.sendKVMessage(serverAnswer);
				} else {
					printError("Invalid request status : " + requestFromClient.getStatus());	
				}
			} catch (IOException e){
				stop = true;
				printError("A connection error occurred - Application terminated " + e);
			}
		}
	}
	
	private KVMessage handleCommand(KVMessage requestToExecute) {
		StatusType requestStatus = requestToExecute.getStatus();
		switch (requestStatus) {
		case GET:
			return sharedCacheManager.get(requestToExecute.getKey());
		case PUT:
			return sharedCacheManager.put(requestToExecute.getKey(), requestToExecute.getValue());
		default:
			return null;
		}	
	}
	
	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

}
