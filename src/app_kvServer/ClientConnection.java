package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.metadata.MetadataHandler;
import client.KVCommModule;

import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

public class ClientConnection implements Runnable {

	protected Socket clientSocket;
	private final MetadataHandler metadataHandler;
	private KVCommModule commModule;
	private final CacheManager sharedCacheManager;
	private static final Logger logger = Logger.getLogger(ClientConnection.class);

	private final boolean stopped;
	private final boolean writeLock;

	public ClientConnection(int port, Socket clientSocket, CacheManager cacheManager,
			MetadataHandler metadataHandler, boolean writeLocked, boolean stopped)
					throws IOException {
		this.writeLock = writeLocked;
		this.stopped = stopped;
		this.clientSocket = clientSocket;
		this.sharedCacheManager = cacheManager;
		this.metadataHandler = metadataHandler;
		this.commModule = new KVCommModule(clientSocket.getOutputStream(),
				clientSocket.getInputStream());
	}

	public void run() {
//		boolean running = true;
//
//		while (running) {
			try {
				KVMessage request = commModule.receiveKVMessage();
				KVMessage serverAnswer = null;

				String key = request.getKey();
				String value = request.getValue();
				logger.debug("Requested from client : "
						+ request);
				if (stopped) {
					serverAnswer = new KVMessageImpl(key, value,
							StatusType.SERVER_STOPPED);
				} else if (!metadataHandler.isResponsibleFor(key)) {
					serverAnswer = new KVMessageImpl(key, metadataHandler.toString(),
							StatusType.SERVER_NOT_RESPONSIBLE);

				} else {
					serverAnswer = handleCommand(key, value,
							request.getStatus());
				}
				logger.debug("Answer to client : "
						+ serverAnswer);

				if (serverAnswer != null) {
					commModule.sendKVMessage(serverAnswer);
				} else {
					logger.error("Invalid answer to request : " + request);
				}
			} catch (IOException e) {
//				running = false;
				logger.error("A connection error occurred - Application terminated "
						+ e);
			} catch (NoSuchAlgorithmException e) {
//				running = false;
				logger.fatal("A hashing error occurred - Application terminated "
						+ e);
			}
//		}
		logger.debug("Client connection terminated.");
	}

	private KVMessage handleCommand(String key, String value,
			StatusType requestStatus) {
		switch (requestStatus) {
		case GET:
			return sharedCacheManager.get(key);
		case PUT:
			if (writeLock) {
				return new KVMessageImpl(key, value,
						StatusType.SERVER_WRITE_LOCK);
			} else {
				return sharedCacheManager.put(key, value);
			}
		default:
			return null;
		}
	}

}
