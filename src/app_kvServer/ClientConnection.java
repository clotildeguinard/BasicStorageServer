package app_kvServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.metadata.MetadataHandler;
import client.KVCommModule;

import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

public class ClientConnection implements Runnable {

	protected Socket clientSocket;
	private static final String PROMPT = "KVServer> ";
	private final MetadataHandler metadataHandler;
	private KVCommModule commModule;
	private final CacheManager sharedCacheManager;
	
	private boolean stop = true;
	private boolean writeLock = false;
	
	public void writeLock() {
		writeLock = true;
	}
	public void writeUnlock() {
		writeLock = false;
	}


    public ClientConnection(int port, Socket clientSocket, CacheManager cacheManager, MetadataHandler metadataHandler) throws UnknownHostException {
        this.clientSocket = clientSocket;
        this.sharedCacheManager = cacheManager;
        this.metadataHandler = metadataHandler;
    	try {
    		commModule = new KVCommModule(clientSocket.getOutputStream(), clientSocket.getInputStream());
        } catch (IOException e1) {
			stop = true;
			printError("A connection error occurred - Application terminated " + e1);
		}
    }
    
    public void run() {
    	stop = false;
    			
		while(!stop) {
			try {
				KVMessage request = commModule.receiveKVMessage();
				KVMessage serverAnswer = null;

					String key = request.getKey();
					String value = request.getValue();
					System.out.println(PROMPT + "Requested from client : " + request);
					if (stop) {
						serverAnswer = new KVMessageImpl(key, value, StatusType.SERVER_STOPPED);
					} else if (!metadataHandler.isResponsibleFor(key)) {
						value = metadataHandler.toString();
						serverAnswer = new KVMessageImpl(key, value, StatusType.SERVER_NOT_RESPONSIBLE);

					} else {
						serverAnswer = handleCommand(key, value, request.getStatus());
					}
					System.out.println(PROMPT + "Answer to client : " + serverAnswer);

				if (serverAnswer != null) {
					commModule.sendKVMessage(serverAnswer);
				} else {
					printError("Invalid answer to request : " + request);	
				}
			} catch (IOException e){
				stop = true;
				printError("A connection error occurred - Application terminated " + e);
			} catch (NoSuchAlgorithmException e) {
				stop = true;
				printError("A hashing error occurred - Application terminated " + e);
			}
		}
	}

	private KVMessage handleCommand(String key, String value, StatusType requestStatus) {
		switch (requestStatus) {
		case GET:
			return sharedCacheManager.get(key);
		case PUT:
			if (writeLock) {
				return new KVMessageImpl(key, value, StatusType.SERVER_WRITE_LOCK);
			} else {
				return sharedCacheManager.put(key, value);
			}
		default:
			return null;
		}	
	}
	
	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

}
