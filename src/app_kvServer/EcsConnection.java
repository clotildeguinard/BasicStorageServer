//package app_kvServer;
//
//import java.io.IOException;
//import java.net.InetAddress;
//import java.net.Socket;
//import java.net.UnknownHostException;
//
//import common.messages.KVMessage;
//import common.messages.KVMessage.StatusType;
//import common.messages.KVMessageImpl;
//import common.metadata.MetadataHandler;
//import client.KVCommModule;
//
//import java.security.NoSuchAlgorithmException;
//
//public class EcsConnection implements Runnable {
//
//	protected Socket ecsSocket;
//	private static final String PROMPT = "KVServer> ";
//	private KVCommModule commModule;
//	private final String serverIp;
//	private final int serverPort;
//	private String minHashKey;
//	private String maxHashKey;
//	
//	private boolean stop = true;
//	private boolean writeLock = false;
//	
//	public void writeLock() {
//		writeLock = true;
//	}
//	public void writeUnlock() {
//		writeLock = false;
//	}
//
//
//    public EcsConnection(int port, Socket ecsSocket) throws UnknownHostException {
//        this.ecsSocket = ecsSocket;
//        this.serverIp = InetAddress.getLocalHost().getHostAddress();
//        this.serverPort = port;
//        updateHashKeyRange();
//    	try {
//    		commModule = new KVCommModule(ecsSocket.getOutputStream(), ecsSocket.getInputStream());
//        } catch (IOException e1) {
//			stop = true;
//			printError("A connection error occurred - Application terminated " + e1);
//		}
//    }
//    
//    public void updateHashKeyRange() {
//    	try {
//			String[] hashKeyBounds = MetadataHandler.getHashKeyBounds("./src/app_kvServer/metadata.txt", serverIp, serverPort);
//			minHashKey = hashKeyBounds[0];
//			maxHashKey = hashKeyBounds[1];
//        } catch (IOException e2) {
//			stop = true;
//			printError("An error occurred when trying to read metadata - Application terminated " + e2);
//		}
//    }
//    
//    public void run() {
//    	stop = false;
//    			
//		while(!stop) {
//			try {
//				KVMessage request = commModule.receiveKVMessage();
//				System.out.println(PROMPT + "Requested from ECS : " + request);
//				
//				String key = request.getKey();
//				String value = request.getValue();
//				KVMessage serverAnswer = handleCommand(key, value, request.getStatus());
//				System.out.println(PROMPT + "Answer to ECS : " + serverAnswer);
//				
//				if (serverAnswer != null) {
//					commModule.sendKVMessage(serverAnswer);
//				} else {
//					printError("Invalid answer to request : " + request);	
//				}
//			} catch (IOException e){
//				stop = true;
//				printError("A connection error occurred - Application terminated " + e);
//			}
//		}
//	}
//
//	private KVMessage handleCommand(String key, String value, StatusType requestStatus) {
//		switch (requestStatus) {
//		case SERVER_WRITE_LOCK:
//			return sharedCacheManager.get(key);
//		case PUT:
//			if (writeLock) {
//				return new KVMessageImpl(key, value, StatusType.SERVER_WRITE_LOCK);
//			} else {
//				return sharedCacheManager.put(key, value);
//			}
//		default:
//			return null;
//		}	
//	}
//	
//	private void printError(String error){
//		System.out.println(PROMPT + "Error! " +  error);
//	}
//
//}
