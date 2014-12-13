package app_kvServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import common.communication.KVAdminCommModule;
import common.communication.KVCommModule;
import common.communication.KVSocketListener;
import common.communication.KVSocketListener.SocketStatus;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.KVMessage;
import common.messages.KVAdminMessage.StatusType;

public class EcsConnection implements Runnable {
	private final KVServer kvServer;
	protected Socket ecsSocket;
	private KVAdminCommModule commModule;
	private final static Logger logger = Logger.getLogger(EcsConnection.class);

	private boolean stopECSConnection = true;
	private boolean hasToShutdownServer = false;

	public EcsConnection(int port, Socket ecsSocket, KVServer kvServer) throws UnknownHostException {
		this.kvServer = kvServer;
		this.ecsSocket = ecsSocket;
		try {
			commModule = new KVAdminCommModule(ecsSocket.getOutputStream(), ecsSocket.getInputStream());
		} catch (IOException e1) {
			stopECSConnection = true;
			logger.error("A connection error occurred - Application terminated " + e1);
		}
	}

	public void run() {
		stopECSConnection = false;

		while(!stopECSConnection) {
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
				stopECSConnection = true;
				hasToShutdownServer = true;
				logger.fatal("An error occurred in ECS connection " + e);
			} finally {
				if (hasToShutdownServer) {
					kvServer.shutdown();
				}
			}
		}
		try {
			tearDownConnection();
		} catch (IOException e) {
			logger.error("An error occurred when tearing down the connection \n" + e );
		}
		
	}

	private KVAdminMessage handleCommand(String key, String value, StatusType statusType) throws IOException {
		switch (statusType) {

		case INIT_KVSERVER:
			String[] parameters = value.split(":");
			int cacheSize = Integer.parseInt(parameters[0]);
			try {
				kvServer.initKVServer(key, cacheSize, parameters[1]);
			} catch (IOException e) {
				return new KVAdminMessageImpl(null, null, StatusType.INIT_KVSERVER);
			}
			return new KVAdminMessageImpl("ok", null, StatusType.INIT_KVSERVER);

		case START:
			kvServer.start();
			return new KVAdminMessageImpl("ok", null, StatusType.START);

		case UPDATE_METADATA:
			kvServer.updateMetadata(value);
			return new KVAdminMessageImpl("ok", null, StatusType.UPDATE_METADATA);

		case MOVE_DATA:
			String[] destinationServer = value.split(":");
			try {
				kvServer.moveData(key, destinationServer[0], Integer.parseInt(destinationServer[1]));
			} catch (IOException e) {
				return new KVAdminMessageImpl(null, null, StatusType.MOVE_DATA);
			}
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
			stopECSConnection = true;
			hasToShutdownServer = true;
			logger.info("Exiting");
			return new KVAdminMessageImpl("ok", null, StatusType.SHUTDOWN);

		default:
			logger.warn("The instruction has an unknown status : " + statusType);
		}
		return null;	
	}
	
	private void tearDownConnection() throws IOException {

		if (ecsSocket != null) {
			commModule.closeStreams();
			ecsSocket.close();
			ecsSocket = null;
			logger.info("Connection closed with ECS.");
		}
	}

}
