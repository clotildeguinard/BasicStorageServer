package client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import client.KVSocketListener.SocketStatus;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;
import common.metadata.MetadataHandler;
import common.metadata.NodeData;

/**
 * send requests to the server, requires call to the defined communication
 * protocol (encode, decode)
 * 
 * @author Clotilde
 * 
 */

public class KVStore extends Thread implements KVCommInterface {

	private KVCommModule commModule;
	private static final Logger logger = Logger.getLogger(KVStore.class);
	private Socket clientSocket;
	private Set<KVSocketListener> listeners;
	private boolean running = false;
	private MetadataHandler metadataHandler;
	private static final int MAX_TRIALS = 3;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore(String defaultIp, int defaultPort) throws IllegalArgumentException {
		this.metadataHandler = new MetadataHandler(defaultIp, defaultPort);
		metadataHandler.update("node0;" + defaultIp + ";" + defaultPort + ";'';''");
	}


	public void addListener(KVSocketListener listener) {
		listeners.add(listener);
	}

	/**
	 * Connect to any known server
	 * 
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public void connect() throws UnknownHostException, IOException {
		NodeData n = metadataHandler.getRandom();
		connect(n.getIpAddress(), n.getPortNumber());
	}

	private void connect(String address, int port) throws IOException {
		logger.debug("Trying to connect to ip " + address + " , port " + port);
		clientSocket = new Socket(address, port);
		commModule = new KVCommModule(clientSocket.getOutputStream(),
				clientSocket.getInputStream());
		listeners = new HashSet<KVSocketListener>();
		addListener(commModule);
		setRunning(true);
		new Thread(this).start();
	}

	private void setRunning(boolean isRunning) {
		running = isRunning;
	}

	public boolean isRunning() {
		return running;
	}


	public void disconnect() {
		logger.debug("try to close connection ...");

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
		setRunning(false);
		logger.debug("tearing down the connection ...");

		if (clientSocket != null) {
			commModule.closeStreams();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}


	/**
	 * Initializes and starts the client connection. Loops until the connection
	 * is closed or aborted by the client.
	 */
	public void run() {
		try {

			while (isRunning()) {

				try {
					TextMessage latestMsg = commModule.receiveMessage();
					for (KVSocketListener listener : listeners) {
						listener.handleNewMessage(latestMsg);
					}
				} catch (IOException ioe) {
					if (isRunning()) {
						logger.error("Connection lost!");
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
			logger.info("KVStore stopped");
		} finally {
			if (isRunning()) {
				disconnect();
			}
		}
	}


	public KVMessage putComm(String key, String value) throws IOException,
	InterruptedException {

		KVMessage msg = new KVMessageImpl(key, value,
				common.messages.KVMessage.StatusType.PUT);
		commModule.sendKVMessage(msg);

		int i = 0;
		while (i < 5 && commModule.latestIsNull()) {
			Thread.sleep(50);
			i++;
		}
		if (i == 5) {
			return null;
		}
		return commModule.getLatest();
	}

	public KVMessage putBis(String key, String value, int nbTrials)
			throws NoSuchAlgorithmException, UnknownHostException, IOException, InterruptedException {
		if (nbTrials > MAX_TRIALS) {
			logger.warn("Responsible server for key " + key + " could not be found after "  + MAX_TRIALS + " trials.");
			return new KVMessageImpl(key, null, StatusType.PUT_ERROR);
		}

		String[] serverForKey = metadataHandler.getServerForKey(key);

		if (serverForKey != null) {
			int portNumber = Integer.parseInt(serverForKey[1]);
			connect(serverForKey[0], portNumber);
		} else {
			connect();
		}

		KVMessage answer = this.putComm(key, value);

		disconnect();
		if (answer == null || answer.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
			return answer;
		}
		metadataHandler.update(answer.getValue());
		return putBis(key, value, nbTrials + 1);

	}

	@Override
	public KVMessage put(String key, String value) throws IOException,
	InterruptedException, NoSuchAlgorithmException {
		KVMessage answer = putBis(key, value, 1);
		logger.debug("answer to put : " + answer);
		return answer;
	}

	/**
	 * returns kvmessage, or null if no kvmessage arrives within a given time
	 * interval
	 */

	public KVMessage getComm(String key) throws IOException,
	InterruptedException {

		KVMessage msg = new KVMessageImpl(key, null,
				common.messages.KVMessage.StatusType.GET);
		commModule.sendKVMessage(msg);

		int i = 0;
		while (i < 5 && commModule.latestIsNull()) {
			Thread.sleep(50);
			i++;
		}
		if (i == 5) {
			return null;
		}
		return commModule.getLatest();
	}

	public KVMessage getBis(String key, int nbTrials)
			throws NoSuchAlgorithmException, UnknownHostException, IOException, InterruptedException {
		if (nbTrials > MAX_TRIALS) {
			logger.warn("Responsible server for key " + key + " could not be found after "  + MAX_TRIALS + " trials.");
			return new KVMessageImpl(key, null, StatusType.GET_ERROR);
		}

		String[] serverForKey = metadataHandler.getServerForKey(key);

		if (serverForKey != null) {
			int portNumber = Integer.parseInt(serverForKey[1]);
			connect(serverForKey[0], portNumber);
		} else {
			connect();
		}

		KVMessage answer = this.getComm(key);
		disconnect();

		if (answer == null || answer.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
			return answer;
		}
		metadataHandler.update(answer.getValue());
		return getBis(key, nbTrials + 1);
	}

	@Override
	public KVMessage get(String key) throws IOException, InterruptedException,
	NoSuchAlgorithmException {

		return getBis(key, 1);

	}
}
