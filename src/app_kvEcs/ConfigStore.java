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
	private final String serverIp;
	private final int serverPort;

	private static final Logger logger = Logger.getLogger(ConfigStore.class);
	private static final int MAX_TRIALS = 3;
	private static final int STANDARD_WAITING_MS = 3000;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 * @throws IOException 
	 */
	public ConfigStore(String ip, int port) throws IllegalArgumentException, IOException {
		this.serverIp = ip;
		this.serverPort = port;
		connect();
	}

	public String[] getIpAndPort() {
		return new String[] {serverIp, Integer.toString(serverPort)};
	}

	public void addListener(KVSocketListener listener) {
		listeners.add(listener);
	}

	private void connect() throws IOException {
		ecsSocket = new Socket(serverIp, serverPort);
		commModule = new KVAdminCommModule(ecsSocket.getOutputStream(),
				ecsSocket.getInputStream());
		logger.info("Connected to " + serverIp + ":" + serverPort);
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
			logger.info("Connection closed with " + serverIp + ":" + serverPort);
		}
	}

	/**
	 * Initializes and starts the connection with server. Loops until the connection
	 * is closed or aborted by the server.
	 */
	public void run() {
		try {

			while (isRunning) {

				try {
					TextMessage latestMsg = commModule.receiveMessage();
					for (KVSocketListener listener : listeners) {
						listener.handleNewMessage(latestMsg);
					}
				} catch (IOException ioe) {
					if (isRunning) {
						logger.error("Connection lost! \n" );
						ioe.printStackTrace();
						try {
							tearDownConnection();
							for (KVSocketListener listener : listeners) {
								listener.handleStatus(SocketStatus.CONNECTION_LOST);
							}
						} catch (IOException e) {
							logger.error("Unable to close connection!");
						}
					}
				}
			}
			logger.info("Connection with server stopped");
		} finally {
			if (isRunning) {
				disconnect();
			}
		}
	}

	public KVAdminMessage sendAndWaitAnswer(KVAdminMessage request, int max_waiting_ms) {
		for (int i = 0; i < MAX_TRIALS ; i++) {
			try {
				commModule.sendKVAdminMessage(request);
				break;
			} catch (IOException e) {
				if (i == MAX_TRIALS - 1) {
					logger.warn("Was unable to send request. \n" + e);
					return null;
				}
			}
		}

		int t = 0;
		while (t < max_waiting_ms && commModule.latestIsNull()) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				logger.warn("Waiting for answer was interrupted !");
				return null;
			}
			t += 50;
		}
		if (t >= max_waiting_ms) {
			return null;
		}
		return commModule.getLatest();
	}

	@Override
	public boolean updateMetadata(String metadata) throws IOException,
	InterruptedException {

		KVAdminMessage msg = new KVAdminMessageImpl(null, metadata,
				common.messages.KVAdminMessage.StatusType.UPDATE_METADATA);
		KVAdminMessage answer = sendAndWaitAnswer(msg, STANDARD_WAITING_MS);
		return (answer != null && answer.getKey() != null && answer.getKey().equals("ok"));
	}


	@Override
	public boolean lockWrite() {
		KVAdminMessage msg = new KVAdminMessageImpl(null, null,
				common.messages.KVAdminMessage.StatusType.LOCK_WRITE);
		KVAdminMessage answer = sendAndWaitAnswer(msg, STANDARD_WAITING_MS);
		return (answer != null && answer.getKey() != null && answer.getKey().equals("ok"));
	}


	@Override
	public boolean unlockWrite() {
		KVAdminMessage msg = new KVAdminMessageImpl(null, null,
				common.messages.KVAdminMessage.StatusType.UNLOCK_WRITE);
		KVAdminMessage answer = sendAndWaitAnswer(msg, STANDARD_WAITING_MS);
		return (answer != null && answer.getKey() != null && answer.getKey().equals("ok"));
	}


	@Override
	public boolean shutdown() {
		try {
			KVAdminMessage msg = new KVAdminMessageImpl(null, null,
					common.messages.KVAdminMessage.StatusType.SHUTDOWN);
			KVAdminMessage answer = sendAndWaitAnswer(msg, STANDARD_WAITING_MS);
			return (answer != null && answer.getKey() != null && answer.getKey().equals("ok"));
		} finally {
			disconnect();
		}
	}

	@Override
	public boolean moveData(String hashOfNewServer, String[] destinationServer) {
		KVAdminMessage msg = new KVAdminMessageImpl(hashOfNewServer, destinationServer[0] + ":" + destinationServer[1],
				common.messages.KVAdminMessage.StatusType.MOVE_DATA);
		KVAdminMessage answer = sendAndWaitAnswer(msg, 60000);
		return (answer != null && answer.getKey() != null && answer.getKey().equals("ok"));
	}

	@Override
	public boolean initKVServer(String metadata, int cacheSize,
			Strategy strategy) {
		KVAdminMessage msg = new KVAdminMessageImpl(metadata, cacheSize + ":" + strategy,
				common.messages.KVAdminMessage.StatusType.INIT_KVSERVER);
		KVAdminMessage answer = sendAndWaitAnswer(msg, STANDARD_WAITING_MS);
		return (answer != null && answer.getKey() != null && answer.getKey().equals("ok"));
	}

	@Override
	public boolean stopServer() {
		KVAdminMessage msg = new KVAdminMessageImpl(null, null,
				common.messages.KVAdminMessage.StatusType.STOP);
		KVAdminMessage answer = sendAndWaitAnswer(msg, STANDARD_WAITING_MS);
		return (answer != null && answer.getKey() != null && answer.getKey().equals("ok"));
	}

	@Override
	public boolean startServer() {
		KVAdminMessage msg = new KVAdminMessageImpl(null, null,
				common.messages.KVAdminMessage.StatusType.START);
		KVAdminMessage answer = sendAndWaitAnswer(msg, STANDARD_WAITING_MS);
		return (answer != null && answer.getKey() != null && answer.getKey().equals("ok"));
	}

}
