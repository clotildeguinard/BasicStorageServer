package app_kvEcs;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import app_kvServer.cache_strategies.Strategy;
import common.communication.AdminCommModule;
import common.communication.SocketListener;
import common.communication.SocketListener.SocketStatus;
import common.messages.AdminMessage;
import common.messages.AdminMessageImpl;
import common.messages.KVMessage.StatusType;
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

	private AdminCommModule commModule;
	private Socket ecsSocket;
	private Set<SocketListener> listeners;
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
	}

	@Override
	public Address getServerAddress() {
		return serverAddress;
	}

	public void addListener(SocketListener listener) {
		listeners.add(listener);
	}

	public void connect() throws IOException {
		ecsSocket = new Socket(serverAddress.getIp(), serverAddress.getPort());
		commModule = new AdminCommModule(ecsSocket.getOutputStream(),
				ecsSocket.getInputStream());
		logger.info("Connected to " + serverAddress);
		listeners = new HashSet<SocketListener>();
		addListener(commModule);
		isRunning = true;
		(new Thread(this)).start();
	}

	private void disconnect() {
		try {
			tearDownConnection();
			if (listeners != null) {
				for (SocketListener listener : listeners) {
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
					// TODO improve
					AdminMessage msg = AdminMessageImpl.unmarshal(latestMsg);
					if (msg.getStatus().equals(common.messages.AdminMessage.StatusType.SHUTDOWN)) {
						disconnect();
					}
					for (SocketListener listener : listeners) {
						listener.handleNewMessage(latestMsg);
					}
				} catch (IOException ioe) {
					if (isRunning) {
						connectionLostError = true;
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
					for (SocketListener listener : listeners) {
						listener.handleStatus(SocketStatus.CONNECTION_LOST);
					}
				}
			} else {
				if (isRunning) {
					disconnect();
				}
			}
		}
	}

	public boolean sendWaitAndCheckAnswer(AdminMessage request, int max_waiting_ms) {
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
		AdminMessage answer = commModule.getLatest();
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

		AdminMessage msg = new AdminMessageImpl(null, metadata,
				common.messages.AdminMessage.StatusType.UPDATE_METADATA);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);

	}

	@Override
	public boolean lockWrite() {
		AdminMessage msg = new AdminMessageImpl(null, null,
				common.messages.AdminMessage.StatusType.LOCK_WRITE);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);

	}

	@Override
	public boolean unlockWrite() {
		AdminMessage msg = new AdminMessageImpl(null, null,
				common.messages.AdminMessage.StatusType.UNLOCK_WRITE);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);

	}

	@Override
	public boolean shutdown() {
		try {
			AdminMessage msg = new AdminMessageImpl(null, null,
					common.messages.AdminMessage.StatusType.SHUTDOWN);
			return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);

		} finally {
			if (isRunning) {
				disconnect();
			}
		}
	}

	@Override
	public boolean moveData(Address destinationServer, String minHashToMove, String maxHashToMove) {
		AdminMessage msg = new AdminMessageImpl(minHashToMove + ":" + maxHashToMove, destinationServer.toString(),
				common.messages.AdminMessage.StatusType.MOVE_DATA);
		return sendWaitAndCheckAnswer(msg, 60000);

	}

	@Override
	public boolean copyData(Address destinationServer, String minHashToMove,
			String maxHashToMove) {
		AdminMessage msg = new AdminMessageImpl(minHashToMove + ":" + maxHashToMove, destinationServer.toString(),
				common.messages.AdminMessage.StatusType.COPY_DATA);
		return sendWaitAndCheckAnswer(msg, 60000);

	}

	@Override
	public boolean initKVServer(String metadata, int cacheSize,
			Strategy strategy) {
		AdminMessage msg = new AdminMessageImpl(metadata, cacheSize + ":" + strategy,
				common.messages.AdminMessage.StatusType.INIT_KVSERVER);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);

	}

	@Override
	public boolean stopServer() {
		AdminMessage msg = new AdminMessageImpl(null, null,
				common.messages.AdminMessage.StatusType.STOP);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);

	}

	@Override
	public boolean startServer() {
		AdminMessage msg = new AdminMessageImpl(null, null,
				common.messages.AdminMessage.StatusType.START);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);

	}

	@Override
	public boolean startHeartbeat() {
		AdminMessage msg = new AdminMessageImpl(null, null,
				common.messages.AdminMessage.StatusType.START_HEARTBEAT);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);

	}

	@Override
	public boolean stopHeartbeat() {
		AdminMessage msg = new AdminMessageImpl(null, null,
				common.messages.AdminMessage.StatusType.STOP_HEARTBEAT);
		return sendWaitAndCheckAnswer(msg, STANDARD_WAITING_MS);

	}
}