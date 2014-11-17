package app_kvServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import common.messages.KVAdminMessage;
import common.messages.KVMessage;
import common.messages.KVAdminMessage.StatusType;
import client.KVCommModule;

public class EcsConnection implements Runnable {
	private final KVServer kvServer;
	protected Socket ecsSocket;
	private static final String PROMPT = "KVServer> ";
	private KVCommModule commModule;
	private final String serverIp;
	private final int serverPort;
	
	private boolean stop = true;



    public EcsConnection(int port, Socket ecsSocket, KVServer kvServer) throws UnknownHostException {
    	this.kvServer = kvServer;
        this.ecsSocket = ecsSocket;
        this.serverIp = InetAddress.getLocalHost().getHostAddress();
        this.serverPort = port;
    	try {
    		commModule = new KVCommModule(ecsSocket.getOutputStream(), ecsSocket.getInputStream());
        } catch (IOException e1) {
			stop = true;
			printError("A connection error occurred - Application terminated " + e1);
		}
    }
    
    public void run() {
    	stop = false;
    			
		while(!stop) {
			try {
//				KVAdminMessage request = commModule.receiveKVMessage();
				KVAdminMessage request = null;
				System.out.println(PROMPT + "Requested from ECS : " + request);
				
				String key = request.getKey();
				String value = request.getValue();
				KVAdminMessage serverAnswer = handleCommand(key, value, request.getStatus());
				System.out.println(PROMPT + "Answer to ECS : " + serverAnswer);
				
				if (serverAnswer != null) {
//					commModule.sendKVMessage(serverAnswer);
				} else {
					printError("Invalid answer to request : " + request);	
				}
			} catch (IOException e){
				stop = true;
				printError("A connection error occurred - Application terminated " + e);
			}
		}
	}

	private KVAdminMessage handleCommand(String key, String value, StatusType statusType) throws IOException {
		switch (statusType) {
		case LOCK_WRITE:
			kvServer.lockWrite();
			break;
		case MOVE_DATA:
			String[] destinationServer = value.split(";");
//			kvServer.moveData(key, destinationServer); // key is the hash of the new server
			break;
		case SHUTDOWN:
			kvServer.shutdown();
			break;
		case START:
			kvServer.start();
			break;
		case STOP:
			kvServer.stop();
			break;
		case UNLOCK_WRITE:
			kvServer.unLockWrite();
			break;
		case UPDATE_METADATA:
			kvServer.update(value);;
			break;
		default:
			return null;
		}
		// TODO
		return null;	
	}
	
	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

}
