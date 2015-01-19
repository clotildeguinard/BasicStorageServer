package app_kvServer;

import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.communication.AdminCommModule;
import common.messages.AdminMessage;
import common.messages.AdminMessageImpl;
import common.messages.AdminMessage.StatusType;
import common.messages.KVMessageImpl;
import common.metadata.Address;

public class EcsConnection extends Thread {
	private final KVServer kvServer;
	protected Socket ecsSocket;
	private AdminCommModule commModule;
	private final static Logger logger = Logger.getLogger(EcsConnection.class);

	private boolean stopECSConnection = false;
	private boolean hasToShutdownServer = false;

	public EcsConnection(int port, Socket ecsSocket, KVServer kvServer) throws IOException {
		this.kvServer = kvServer;
		this.ecsSocket = ecsSocket;
		commModule = new AdminCommModule(ecsSocket.getOutputStream(), ecsSocket.getInputStream());
	}

	public void denounceSuspNode(AdminMessage suspicionMsg) {
		try {
			commModule.sendKVAdminMessage(suspicionMsg);
		} catch (IOException e) {
		}
	}

	public void run() {
		boolean hasToStartOtherEcsConnection = false;
		boolean hasToKeepConnectionAlive = false;

		try {

			while(!stopECSConnection) {
				AdminMessage request = commModule.receiveKVAdminMessage();
				logger.info("Requested from ECS : " + request);

				String key = request.getKey();
				String value = request.getValue();
				AdminMessage serverAnswer = handleCommand(key, value, request.getStatus());
				logger.info("Answer to ECS : " + serverAnswer);

				if (serverAnswer != null) {
					commModule.sendKVAdminMessage(serverAnswer);
				} else {
					logger.warn("Invalid answer to request : " + request);	
				}
			}

		} catch (IllegalStateException e) {
			logger.warn(e.getMessage());
			try {
				kvServer.transferRequestToClient(ecsSocket,
						KVMessageImpl.unmarshal(commModule.getLatestXmlTxt()));
			} catch (IOException e1) {
				logger.warn("Client request could not be transferred"
						+ " from ecsCommModule to clientCommModule.");
			}
			hasToKeepConnectionAlive = true;
			hasToStartOtherEcsConnection = true;
		} catch (IOException e){
			logger.fatal("Must shut down the server because of IOException : " + e.getMessage());
			hasToShutdownServer = true;
		}
		finally {
			try {
				if (!hasToKeepConnectionAlive) {
					tearDownConnection();
				}
			} catch (IOException e) {
				logger.error("An error occurred when tearing down the connection \n" + e );
			}
			if (hasToShutdownServer) {
				kvServer.shutdown();
			} else if (hasToStartOtherEcsConnection) {
				kvServer.startEcsConnection();
			}
		}
	}


	private AdminMessage handleCommand(String key, String value, StatusType statusType) throws IOException {
		switch (statusType) {

		case INIT_KVSERVER:
			String[] parameters = value.split(":");
			int cacheSize = Integer.parseInt(parameters[0]);
			try {
				kvServer.initKVServer(key, cacheSize, parameters[1]);
			} catch (IOException e) {
				return new AdminMessageImpl(null, null, StatusType.INIT_KVSERVER);
			}
			return new AdminMessageImpl("ok", null, StatusType.INIT_KVSERVER);

		case START:
			kvServer.start();
			return new AdminMessageImpl("ok", null, StatusType.START);

		case UPDATE_METADATA:
			kvServer.updateMetadata(value);
			return new AdminMessageImpl("ok", null, StatusType.UPDATE_METADATA);

		case MOVE_DATA:
			String[] destination = value.split(":");
			String[] rangeToMove = key.split(":");
			AdminMessage answer = null;
			try {
				answer = copyData(rangeToMove, new Address(destination[0], Integer.parseInt(destination[1])));
				kvServer.deleteData(rangeToMove[0], rangeToMove[1]);
			} catch (IOException e) {
				return new AdminMessageImpl(null, null, StatusType.MOVE_DATA);
			}
			return answer;

		case COPY_DATA:
			String[] destin = value.split(":");
			String[] range2Move = key.split(":");
			try {
				return copyData(range2Move, new Address(destin[0], Integer.parseInt(destin[1])));
			} catch (IOException e) {
				return new AdminMessageImpl(null, null, StatusType.MOVE_DATA);
			}

		case LOCK_WRITE:
			kvServer.lockWrite();
			return new AdminMessageImpl("ok", null, StatusType.LOCK_WRITE);

		case UNLOCK_WRITE:
			kvServer.unLockWrite();
			return new AdminMessageImpl("ok", null, StatusType.UNLOCK_WRITE);

		case START_HEARTBEAT:
			kvServer.startHeartbeat();
			return new AdminMessageImpl("ok", null, StatusType.START_HEARTBEAT);

		case STOP_HEARTBEAT:
			kvServer.stopHeartbeat();
			return new AdminMessageImpl("ok", null, StatusType.STOP_HEARTBEAT);

		case STOP:
			kvServer.stop();
			return new AdminMessageImpl("ok", null, StatusType.STOP);

		case SHUTDOWN:
			stopECSConnection = true;
			hasToShutdownServer = true;
			logger.info("Exiting");
			return new AdminMessageImpl("ok", null, StatusType.SHUTDOWN);

		default:
			logger.warn("The instruction has an unknown status : " + statusType);
		}
		return null;	
	}

	private AdminMessage copyData(String[] rangeToMove, Address destinationServer) throws NumberFormatException, IOException {
		kvServer.copyData(rangeToMove[0], rangeToMove[1], destinationServer);
		return new AdminMessageImpl("ok", null, StatusType.MOVE_DATA);
	}

	private void tearDownConnection() throws IOException {
		stopECSConnection = true;

		if (ecsSocket != null) {
			commModule.closeStreams();
			ecsSocket.close();
			ecsSocket = null;
			logger.info("Connection closed with ECS.");
		}
	}
}