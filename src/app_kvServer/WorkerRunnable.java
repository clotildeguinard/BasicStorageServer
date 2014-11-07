package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import client.KVCommModule;
import logger.ClientLogSetup;

public class WorkerRunnable implements Runnable {

	protected Socket clientSocket = null;
    protected String serverText   = null;
	private static final String PROMPT = "KVServer> ";
	private KVCommModule commModule;
	private boolean stop = true;
	private CacheManager sharedCacheManager;

    public WorkerRunnable(Socket clientSocket, String serverText, CacheManager cacheManager) {
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
				System.out.print(PROMPT + "Requested from client : " + requestFromClient.getStatus() + 
						" " + requestFromClient.getKey() + " " + requestFromClient.getValue());
				
				KVMessage serverAnswer = handleCommand(requestFromClient);
				
				System.out.print(PROMPT + "Answer to client : " + serverAnswer.getStatus() + 
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

//    public void run() {
//        try {
//            InputStream input  = clientSocket.getInputStream();
//            OutputStream output = clientSocket.getOutputStream();
//            long time = System.currentTimeMillis();
//            output.write(("HTTP/1.1 200 OK\n\nWorkerRunnable: " +
//this.serverText + " - " +
//time +
//"").getBytes());
//            output.close();
//            input.close();
//            System.out.println("Request processed: " + time);
//        } catch (IOException e) {
//            //report exception somewhere.
//            e.printStackTrace();
//        }
//    }

}
