package app_kvEcs;

import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import app_kvServer.cache_strategies.Strategy;
import common.communication.KVAdminCommModule;
import common.communication.KVSocketListener;
import common.communication.KVSocketListener.SocketStatus;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.TextMessage;
import common.metadata.Address;

/**
 * send requests to the server, requires call to the defined communication
 * protocol (encode, decode)
 * 
 * @author Clotilde
 * 
 */

public class ConfigStore extends Thread implements ConfigCommInterface {

	private KVAdminCommModule commModule;
	private Socket ecsSocket;
	private Set<KVSocketListener> listeners;
	private boolean isRunning = false;
	private final Address serverAddress;

	private static final Logger logger = Logger.getLogger(ConfigStore.class);
	private static final int MAX_TRIALS = 3;
	private static final int STANDARD_WAITING_MS = 3000;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param ip
	 *            the ip of the KVServer
	 * @param port
	 *            the port of the KVServer
	 * @throws IOException 
	 */
	public ConfigStore(String ip, int port) throws IllegalArgumentException, IOException {
		this.serverAddress = new Address(ip, port);
		connect();
	}
	
	public Address getServerAddress() {
		return serverAddress;
	}

	public void addListener(KVSocketListener listener) {
		listeners.add(listener);
	}

	private void connect() throws IOException {
		ecsSocket = new Socket(serverAddress.getIp(), serverAddress.getPort());
		commModule = new KVAdminCommModule(ecsSocket.getOutputStream(),
				ecsSocket.getInputStream());
		logger.info("Connected to " + serverAddress);
		listeners = new HashSet<KVSocketListener>();
		addListener(commModule);
		isRunning = true;
		new Thread(this).start();
	}

	public void disconnect() {
		try {
			tearDownConnection();
			if (listeners != null) {
				for (KVSocketListener listener : listeners) {
					listener.handleStatus(SocketStatus.DISCONNECTED);
				}
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	private void tearDownConnection() throws IOException {
		isRunning = false;

		if (ecsSocket != null) {
			commModule.closeStreams();
			ecsSocket.close();
			ecsSocket = null;
			logger.info("Connection closed with " + serverAddress);
		}
	}

	/**
	 * Initializes and starts the connection with server.
	 * Loops until the connection is closed by ecs ("shutdown")
	 * or aborted by the server.
	 */
	public void run() {
		boolean connectionLostError = false;
		try {
			while (isRunning) {
				try {
					TextMessage latestMsg = commModule.receiveMessage();
					for (KVSocketListener listener : listeners) {
						listener.handleNewMessage(latestMsg);
					}
				} catch (IOException ioe) {
					connectionLostError = true;
					if (isRunning) {
						try {
							tearDownConnection();
						} catch (IOException e) {
							isRunning = false;
						}
					}
				}
			}
		} finally {
			if (connectionLostError) {
				try {
					connect();
					connectionLostError = false;
					logger.info("Connection lost and recovered.");
				} catch (IOException e) {
					logger.error("Connection lost, unable to reconnect !");
					for (KVSocketListener listener : listeners) {
						listener.handleStatus(SocketStatus.CONNECTION_LOST);
					}
				}
			} else {
				if (isRunning) {
					disconnect();
				}
				logger.info("Connection with server stopped");
			}
		}
	}

	public boolean sendWaitAndCheckAnswer(KVAdminMessage request, int max_waiting_ms) {
		for (int i = 0; i < MAX_TRIALS ; i++) {
			try {
				commModule.sendKVAdminMessage(request);
				break;
			} catch (IOException e) {
				if (i == MAX_TRIALS - 1) {
					logger.warn("Was unable to send request. \n" + e);
					return false;
				}
			}
		}

		int t = 0;
		while (t < max_waiting_ms && commModule.latestIsNull()) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				logger.warn("Waiting for answer was interrupted !");
				return false;
			}
			t += 50;
		}
		if (t >= max_waiting_ms) {
			return false;
		}
		KVAdminMessage answer = commModule.getLatest();
		if (answer == null || answer.getKey() == null) {
			return false;
		} else if (answer.getKey().equals("ok")) {
			return true;
		} else if (answer.getKey().equals("error")) {
			return sendWaitAndCheckAnswer(request, max_waiting_ms);
		} else {
			return false;
		}
	}

	@Override
	public boolean updateMetadata(String metadata) throws IOException,
	InterruptedException {

		KVAdminMessage msg = new KVAdminMessageImpl(null, metadata,
				common.messages.KVAdminMessage.StatusType.UPDATE_METADATA);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);
		
	}


	@Override
	public boolean lockWrite() {
		KVAdminMessage msg = new KVAdminMessageImpl(null, null,
				common.messages.KVAdminMessage.StatusType.LOCK_WRITE);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);
		
	}


	@Override
	public boolean unlockWrite() {
		KVAdminMessage msg = new KVAdminMessageImpl(null, null,
				common.messages.KVAdminMessage.StatusType.UNLOCK_WRITE);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);
		
	}


	@Override
	public boolean shutdown() {
		try {
			KVAdminMessage msg = new KVAdminMessageImpl(null, null,
					common.messages.KVAdminMessage.StatusType.SHUTDOWN);
			return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);
			
		} finally {
			disconnect();
		}
	}

	@Override
	public boolean moveData(String[] destinationServer, String minHashToMove, String maxHashToMove) {
		KVAdminMessage msg = new KVAdminMessageImpl(minHashToMove + ":" + maxHashToMove, destinationServer[0] + ":" + destinationServer[1],
				common.messages.KVAdminMessage.StatusType.MOVE_DATA);
		return sendWaitAndCheckAnswer(msg, 60000);
		
	}

	@Override
	public boolean copyData(String[] destinationServer, String minHashToMove,
			String maxHashToMove) {
		KVAdminMessage msg = new KVAdminMessageImpl(minHashToMove + ":" + maxHashToMove, destinationServer[0] + ":" + destinationServer[1],
				common.messages.KVAdminMessage.StatusType.COPY_DATA);
		return sendWaitAndCheckAnswer(msg, 60000);
		
	}


	@Override
	public boolean initKVServer(String metadata, int cacheSize,
			Strategy strategy) {
		KVAdminMessage msg = new KVAdminMessageImpl(metadata, cacheSize + ":" + strategy,
				common.messages.KVAdminMessage.StatusType.INIT_KVSERVER);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);
		
	}

	@Override
	public boolean stopServer() {
		KVAdminMessage msg = new KVAdminMessageImpl(null, null,
				common.messages.KVAdminMessage.StatusType.STOP);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);
		
	}

	@Override
	public boolean startServer() {
		KVAdminMessage msg = new KVAdminMessageImpl(null, null,
				common.messages.KVAdminMessage.StatusType.START);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);
		
	}

	@Override
	public boolean startHeartbeat() {
		KVAdminMessage msg = new KVAdminMessageImpl(null, null,
				common.messages.KVAdminMessage.StatusType.START_HEARTBEAT);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);
		
	}

	@Override
	public boolean stopHeartbeat() {
		KVAdminMessage msg = new KVAdminMessageImpl(null, null,
				common.messages.KVAdminMessage.StatusType.STOP_HEARTBEAT);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);
		
	}

}
