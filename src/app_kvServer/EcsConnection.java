package app_kvServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.KVMessage;
import common.messages.KVAdminMessage.StatusType;
import client.KVAdminCommModule;
import client.KVCommModule;

public class EcsConnection implements Runnable {
	private final KVServer kvServer;
	protected Socket ecsSocket;
	private KVAdminCommModule commModule;
	private Logger logger = Logger.getLogger(getClass().getSimpleName());

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
				logger.fatal("An error occurred in ECS connection - Application terminated " + e);
				System.exit(1);
			}
		}
	}

	private KVAdminMessage handleCommand(String key, String value, StatusType statusType) throws IOException {
		switch (statusType) {
		case INIT_KVSERVER:
			String[] parameters = value.split(":");
			int cacheSize = Integer.parseInt(parameters[0]);
			kvServer.initKVServer(key, cacheSize, parameters[1]);
			return new KVAdminMessageImpl("ok", null, StatusType.INIT_KVSERVER);
		case START:
			kvServer.start();
			return new KVAdminMessageImpl("ok", null, StatusType.START);
		case UPDATE_METADATA:
			kvServer.updateMetadata(value);
			return new KVAdminMessageImpl("ok", null, StatusType.UPDATE_METADATA);
		case MOVE_DATA:
			String[] destinationServer = value.split(":");
			kvServer.moveData(key, destinationServer[0], Integer.parseInt(destinationServer[1]));
			return new KVAdminMessageImpl("ok", null, StatusType.MOVE_DATA);
		case LOCK_WRITE:
			kvServer.lockWrite();
			return new KVAdminMessageImpl("ok", null, StatusType.LOCK_WRITE);
		case UNLOCK_WRITE:
			kvServer.unLockWrite();
			return new KVAdminMessageImpl("ok", null, StatusType.UNLOCK_WRITE);
		case STOP:
			kvServer.stop();
			return new KVAdminMessageImpl("ok", null, StatusType.STOP);
		case SHUTDOWN:
			kvServer.stop();
			stop = true;
			logger.info("Exiting");
			return new KVAdminMessageImpl("ok", null, StatusType.SHUTDOWN);
		default:
			logger.warn("The instruction has an unknown status : " + statusType);
		}
		return null;	
	}

}
