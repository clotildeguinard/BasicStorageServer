package app_kvServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.UnknownHostException;

import common.communication.KVCommModule;
import common.communication.KVSocketListener;
import common.communication.KVSocketListener.SocketStatus;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.metadata.MetadataHandler;

import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.log4j.Logger;

public class ClientConnection implements Runnable {

	protected Socket clientSocket;
	private final MetadataHandler metadataHandler;
	private KVCommModule commModule;
	private final CacheManager sharedCacheManager;
	private static final Logger logger = Logger.getLogger(ClientConnection.class);

	private final boolean stopped;
	private final boolean writeLock;
	private final List<KVMessage> heartbeatQueue;

	public ClientConnection(int port, Socket clientSocket, CacheManager cacheManager,
			MetadataHandler metadataHandler, boolean writeLocked, boolean stopped, List<KVMessage> heartbeatQueue)
					throws IOException {
		this.writeLock = writeLocked;
		this.stopped = stopped;
		this.clientSocket = clientSocket;
		this.sharedCacheManager = cacheManager;
		this.metadataHandler = metadataHandler;
		this.commModule = new KVCommModule(clientSocket.getOutputStream(),
				clientSocket.getInputStream());
		this.heartbeatQueue = heartbeatQueue;
	}

	public void run() {
		try {
			KVMessage request = commModule.receiveKVMessage();
			KVMessage serverAnswer = null;

			String key = request.getKey();
			String value = request.getValue();
			logger.info("Requested from client : "
					+ request);
			if (stopped) {
				serverAnswer = new KVMessageImpl(key, value,
						StatusType.SERVER_STOPPED);
			} else {
				serverAnswer = handleCommand(key, value,
						request.getStatus());
			}
			logger.info("Answer to client : "
					+ serverAnswer);

			if (serverAnswer != null) {
				commModule.sendKVMessage(serverAnswer);
			} else if (!request.getStatus().equals(StatusType.HEARTBEAT)){
				logger.error("Invalid answer to request : " + request);
			}
		} catch (IOException e) {
			logger.error("A connection error occurred - Application terminated "
					+ e);
		} catch (NoSuchAlgorithmException e) {
			logger.fatal("A hashing error occurred - Application terminated "
					+ e);
		}
		
		try {
			tearDownConnection();
		} catch (IOException e) {
			logger.error("An error occurred when tearing down the connection \n" + e );
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
			if (!metadataHandler.isWriteResponsibleFor(key)) {
				return new KVMessageImpl(key, metadataHandler.toString(),
						StatusType.SERVER_NOT_RESPONSIBLE);
			} else if (writeLock) {
				return new KVMessageImpl(key, value,
						StatusType.SERVER_WRITE_LOCK);
			} else {
				return sharedCacheManager.put(key, value);
			}
		case HEARTBEAT:
			heartbeatQueue.add(new KVMessageImpl(key, value, StatusType.HEARTBEAT));
			return null;
			
		default:
			return null;
		}
	}

	private void tearDownConnection() throws IOException {
		if (clientSocket != null) {
			commModule.closeStreams();
			clientSocket.close();
			clientSocket = null;
			logger.info("Connection closed.");
		}
	}
}
