package app_kvServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

import app_kvApi.KVStore;
import app_kvApi.KVStoreServer;
import common.communication.KVCommModule;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.metadata.Address;
import common.metadata.MetadataHandlerServer;

import java.security.NoSuchAlgorithmException;
import java.util.Set;

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

	/**
	 * Receive and answer one request,
	 * then tear down connection
	 */
	@Override
	public void run() {
		try {
			KVMessage request = null;
			try {
				request = commModule.receiveKVMessage();
			} catch (IllegalStateException e) {
				logger.warn(e.getMessage());
				tearDownConnection();
				return;
			} catch (IOException e) {
				logger.info("Connection closed by client.");
				tearDownConnection();
			}

			handleRequest(request);

		} catch (SocketException e) {
			logger.error("Connection closed by client.");
			tearDownConnection();
		} catch (IOException e) {
			tearDownConnection();
			logger.error("A connection error occurred - Connection terminated "
					+ e);
		} catch (NoSuchAlgorithmException e) {
			tearDownConnection();
			logger.fatal("A hashing error occurred - Connection terminated "
					+ e);
		}
	}

	/**
	 * Transfer to hearbeatHandler if is heartbeat,
	 * else if stopped send "stopped" flag,
	 * else execute request
	 * @param request
	 * @throws NoSuchAlgorithmException
	 * @throws SocketException
	 * @throws IOException
	 */
	private void handleRequest(KVMessage request) throws NoSuchAlgorithmException, SocketException, IOException {
		String key = request.getKey();
		String value = request.getValue();
		StatusType status = request.getStatus();
		logger.info("Requested from client : "
				+ request);

		if (status == StatusType.HEARTBEAT) {
			if (heartbeatHandler != null) {
				heartbeatHandler.handleReceivedHeartBeat(key, value);
			}
			tearDownConnection();
			return;
		}

		KVMessage serverAnswer = null;

		if (stopped) {
			serverAnswer = new KVMessageImpl(key, value,
					StatusType.SERVER_STOPPED);
		} else {
			serverAnswer = executeCommand(key, value,
					status);
		}
		logger.info("Answer to client : "
				+ serverAnswer);
		commModule.sendKVMessage(serverAnswer);
		tearDownConnection();

		if (status == StatusType.PUT_PROPAGATE
				&& hasToPropagateWriteRequest(serverAnswer.getStatus())) {
			propagateWriteToReplicas(key, value);
		}
	}

	/**
	 * @param answerStatus
	 * @return true if request was executed successfully locally
	 */
	private boolean hasToPropagateWriteRequest(StatusType answerStatus) {
		return answerStatus != StatusType.SERVER_NOT_RESPONSIBLE
				&& answerStatus != StatusType.SERVER_WRITE_LOCK
				&& answerStatus != StatusType.DELETE_ERROR
				&& answerStatus != StatusType.PUT_ERROR;
	}

	/**
	 * Propagate write request to all virtual nodes being read responsible for the key,
	 * except to virtual nodes corresponding to same server (hence, same storage)
	 * @param key
	 * @param value
	 * @throws UnknownHostException
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException 
	 */
	private void propagateWriteToReplicas(String key, String value) throws UnknownHostException, NoSuchAlgorithmException, UnsupportedEncodingException {
		Set<Address> replicas = metadataHandler.getReplicas(key);

		for (Address a : replicas) {
			KVStore kvStore = new KVStoreServer(a);
			try {
				kvStore.connect();
				kvStore.put(key, value);

			} catch (InterruptedException e) {
				logger.error("Could not receive answer of replica because of : " + e.getMessage());
			} catch (IOException e) {
				logger.error("An error occurred during connection to replica because of : " + e.getMessage());
			} finally {
				kvStore.disconnect();
			}
		}
	}

	/**
	 * 
	 * @param key
	 * @param value
	 * @param requestStatus
	 * @return answer of cacheManager if node is responsible, "not_responsible" flag else
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 */
	private KVMessage executeCommand(String key, String value,
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

	private void tearDownConnection() {
		try {
			if (clientSocket != null) {
				commModule.closeStreams();
				clientSocket.close();
				clientSocket = null;
			}
		} catch (IOException e) {
			logger.error("An error occurred when tearing down the connection \n" + e );
		}
	}
}
