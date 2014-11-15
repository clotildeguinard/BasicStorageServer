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

public class ClientConnection implements Runnable {

	protected Socket clientSocket;
	private static final String PROMPT = "KVServer> ";
	private final String metadataLocation;
	private KVCommModule commModule;
	private final String serverIp;
	private final int serverPort;
	private String minHashKey;
	private String maxHashKey;
	private final CacheManager sharedCacheManager;
	
	private boolean stop = true;
	private boolean writeLock = false;
	
	public void writeLock() {
		writeLock = true;
	}
	public void writeUnlock() {
		writeLock = false;
	}


    public ClientConnection(int port, Socket clientSocket, CacheManager cacheManager, String metadataLocation) throws UnknownHostException {
        this.clientSocket = clientSocket;
        this.sharedCacheManager = cacheManager;
        this.metadataLocation = metadataLocation;
        this.serverIp = InetAddress.getLocalHost().getHostAddress();
        this.serverPort = port;
        updateHashKeyRange();
    	try {
    		commModule = new KVCommModule(clientSocket.getOutputStream(), clientSocket.getInputStream());
        } catch (IOException e1) {
			stop = true;
			printError("A connection error occurred - Application terminated " + e1);
		}
    }
    
    public void updateHashKeyRange() {
    	try {
			String[] hashKeyBounds = MetadataHandler.getHashKeyBounds("./src/app_kvServer/metadata.txt", serverIp, serverPort);
			minHashKey = hashKeyBounds[0];
			maxHashKey = hashKeyBounds[1];
        } catch (IOException e2) {
			stop = true;
			printError("An error occurred when trying to read metadata - Application terminated " + e2);
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
					} else if (!MetadataHandler.isInRange(key, minHashKey, maxHashKey)) {
						value = MetadataHandler.getMetadata(metadataLocation);
						serverAnswer = new KVMessageImpl(key, value, StatusType.SERVER_NOT_RESPONSIBLE);
						// TODO
						// send metadata
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
