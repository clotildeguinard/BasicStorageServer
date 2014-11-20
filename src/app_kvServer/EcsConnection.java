package app_kvServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.KVMessage;
import common.messages.KVAdminMessage.StatusType;
import client.KVAdminCommModule;
import client.KVCommModule;

public class EcsConnection implements Runnable {
	private final KVServer kvServer;
	protected Socket ecsSocket;
	private KVAdminCommModule commModule;
	private Logger logger = Logger.getRootLogger();
	
	private boolean stop = true;

    public EcsConnection(int port, Socket ecsSocket, KVServer kvServer) throws UnknownHostException {
    	this.kvServer = kvServer;
        this.ecsSocket = ecsSocket;
    	try {
    		commModule = new KVAdminCommModule(ecsSocket.getOutputStream(), ecsSocket.getInputStream());
        } catch (IOException e1) {
			stop = true;
			logger.error("A connection error occurred - Application terminated " + e1);
		}
    }
    
    public void run() {
    	stop = false;
    			
		while(!stop) {
			try {
				KVAdminMessage request = commModule.receiveKVAdminMessage();
				logger.info("Requested from ECS : " + request);
				
				String key = request.getKey();
				String value = request.getValue();
				KVAdminMessage serverAnswer = handleCommand(key, value, request.getStatus());
				logger.info("Answer to ECS : " + serverAnswer);
				
				if (serverAnswer != null) {
					commModule.sendKVAdminMessage(serverAnswer);
				} else {
					logger.warn("Invalid answer to request : " + request);	
				}
			} catch (IOException e){
				stop = true;
				logger.fatal("A connection error occurred - Application terminated " + e);
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
			kvServer.moveData(key, destinationServer); // key is the maxHashKey of the new server
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
			kvServer.update(value); // value contains the metadata
			break;
		default:
			logger.warn("The instruction has an unknown status : " + statusType);
		}
		return null;	
	}

}
