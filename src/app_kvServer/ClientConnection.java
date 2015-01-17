package app_kvServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.UnknownHostException;

import app_kvApi.KVStore;
import app_kvApi.KVStoreServer;
import common.communication.KVCommModule;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.metadata.MetadataHandlerServer;
import common.metadata.NodeData;

import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

public class ClientConnection implements Runnable {

	protected Socket clientSocket;
	private final MetadataHandlerServer metadataHandler;
	private KVCommModule commModule;
	private final CacheManager sharedCacheManager;
	private static final Logger logger = Logger.getLogger(ClientConnection.class);

	private final boolean stopped;
	private final boolean writeLock;
	private HeartBeatHandler heartbeatHandler;

	public ClientConnection(int port, Socket clientSocket, CacheManager cacheManager,
			MetadataHandlerServer metadataHandler, boolean writeLocked, boolean stopped, HeartBeatHandler heartbeatHandler)
					throws IOException {
		this.writeLock = writeLocked;
		this.stopped = stopped;
		this.clientSocket = clientSocket;
		this.sharedCacheManager = cacheManager;
		this.metadataHandler = metadataHandler;
		this.commModule = new KVCommModule(clientSocket.getOutputStream(),
				clientSocket.getInputStream());
		this.heartbeatHandler = heartbeatHandler;
	}

	public void run() {
		try {
			KVMessage request = commModule.receiveKVMessage();
			KVMessage serverAnswer = null;

			String key = request.getKey();
			String value = request.getValue();
			StatusType status = request.getStatus();
			logger.info("Requested from client : "
					+ request);
			
			if (status == StatusType.HEARTBEAT) {
				heartbeatHandler.handleReceivedHeartBeat(new KVMessageImpl(key, value, StatusType.HEARTBEAT));
				return;
				
			} else if (stopped) {
				serverAnswer = new KVMessageImpl(key, value,
						StatusType.SERVER_STOPPED);
			} else {
				serverAnswer = handleCommand(key, value,
						status);
			}
			logger.info("Answer to client : "
					+ serverAnswer);

			if (serverAnswer != null) {
				commModule.sendKVMessage(serverAnswer);
			} else {
				logger.error("Null answer to request !");
			}

			try {
				tearDownConnection();
			} catch (IOException e) {
				logger.error("An error occurred when tearing down the connection \n" + e );
			}

			if (serverAnswer != null && status == StatusType.PUT_PROPAGATE
					&& hasToPropagateRequest(serverAnswer.getStatus())) {
				propagateToReplicas(key, value);
			}
			
		} catch (IOException e) {
			logger.error("A connection error occurred - Connection terminated "
					+ e);
		} catch (NoSuchAlgorithmException e) {
			logger.fatal("A hashing error occurred - Connection terminated "
					+ e);
		}
	}

	private boolean hasToPropagateRequest(StatusType answerStatus) {
		return answerStatus != StatusType.SERVER_NOT_RESPONSIBLE
				&& answerStatus != StatusType.SERVER_WRITE_LOCK
				&& answerStatus != StatusType.DELETE_ERROR
				&& answerStatus != StatusType.PUT_ERROR;
	}

	private void propagateToReplicas(String key, String value) throws UnknownHostException, IOException, NoSuchAlgorithmException {
		NodeData rep1 = metadataHandler.getReplica1();
		KVStore kvStore = new KVStoreServer(rep1.getAddress());
		try {
			kvStore.connect();
			kvStore.put(key, value);

		} catch (InterruptedException e) {
			logger.error("An error occurred during connection to other server", e);
		} finally {
			kvStore.disconnect();
		}

		NodeData rep2 = metadataHandler.getReplica2();
		kvStore = new KVStoreServer(rep2.getAddress());
		try {
			kvStore.connect();
			kvStore.put(key, value);

		} catch (InterruptedException e) {
			logger.error("An error occurred during connection to other server", e);
		} finally {
			kvStore.disconnect();
		}
	}

	private KVMessage handleCommand(String key, String value,
			StatusType requestStatus) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		switch (requestStatus) {
		case GET:
			if (!metadataHandler.isReadResponsibleFor(key)) {
				return new KVMessageImpl(key, metadataHandler.toString(),
						StatusType.SERVER_NOT_RESPONSIBLE);
			}
			return sharedCacheManager.get(key);
		case PUT:
			if (!metadataHandler.isReadResponsibleFor(key)) {
				return new KVMessageImpl(key, metadataHandler.toString(),
						StatusType.SERVER_NOT_RESPONSIBLE);
			} else if (writeLock) {
				return new KVMessageImpl(key, value,
						StatusType.SERVER_WRITE_LOCK);
			} else {
				return sharedCacheManager.put(key, value);
			}
		case PUT_PROPAGATE:
			if (!metadataHandler.isWriteResponsibleFor(key)) {
				return new KVMessageImpl(key, metadataHandler.toString(),
						StatusType.SERVER_NOT_RESPONSIBLE);
			} else if (writeLock) {
				return new KVMessageImpl(key, value,
						StatusType.SERVER_WRITE_LOCK);
			} else {
				return sharedCacheManager.put(key, value);
			}
			
		default:
			return null;
		}
	}

	private void tearDownConnection() throws IOException {
		if (clientSocket != null) {
			commModule.closeStreams();
			clientSocket.close();
			clientSocket = null;
			logger.debug("Connection closed.");
		}
	}
}
