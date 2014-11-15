package app_kvServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.metadata.MetadataHandler;
import client.KVCommModule;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ClientConnection implements Runnable {

	protected Socket clientSocket;
	private static final String PROMPT = "KVServer> ";
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


    public ClientConnection(int port, Socket clientSocket, CacheManager cacheManager) throws UnknownHostException {
        this.clientSocket = clientSocket;
        this.sharedCacheManager = cacheManager;
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
				if (isClientMessage()) {
					String key = request.getKey();
					String value = request.getValue();
					System.out.println(PROMPT + "Requested from client : " + request);
					if (stop) {
						serverAnswer = new KVMessageImpl(key, value, StatusType.SERVER_STOPPED);
					} else if (!isResponsibleForKey(key)) {
						serverAnswer = new KVMessageImpl(key, value, StatusType.SERVER_NOT_RESPONSIBLE);
						// TODO
						// send metadata
					} else {
						serverAnswer = handleCommand(key, value, request.getStatus());
					}
					System.out.println(PROMPT + "Answer to client : " + serverAnswer);
				} else if (isEcsMessage()){
					System.out.println(PROMPT + "Requested from ECS : " + request);
					// TODO
					// lock or stop
					System.out.println(PROMPT + "Answer to ECS : " + serverAnswer);
				}
				if (serverAnswer != null) {
					commModule.sendKVMessage(serverAnswer);
				} else {
					printError("Invalid request status : " + request.getStatus());	
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
	
	private boolean isEcsMessage() {
		// TODO Auto-generated method stub
		return false;
	}

	private boolean isResponsibleForKey(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		System.out.println(key);
		String hashedKey = new BigInteger(1,MessageDigest.getInstance("MD5").digest(key.getBytes("UTF-8"))).toString(16);
		while(hashedKey.length() < 32 ){
			hashedKey = hashedKey + "0";
			}
		System.out.println(hashedKey);
		System.out.println(minHashKey);
		System.out.println(maxHashKey);
		return hashedKey.compareTo(minHashKey) >= 0 && hashedKey.compareTo(maxHashKey) <= 0;
	}

	private boolean isClientMessage() {
		// TODO Auto-generated method stub
		return true;
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
