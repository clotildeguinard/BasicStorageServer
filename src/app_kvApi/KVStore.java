package app_kvApi;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.communication.KVCommInterface;
import common.communication.KVCommModule;
import common.communication.KVSocketListener;
import common.communication.KVSocketListener.SocketStatus;
import common.messages.KVMessage;
import common.messages.TextMessage;
import common.metadata.Address;
import common.metadata.MetadataHandler;
import common.metadata.NodeData;

/**
 * send requests to the server, requires call to the defined communication
 * protocol (encode, decode)
 * 
 * @author Clotilde
 * 
 */

public abstract class KVStore extends Thread implements KVCommInterface {

	private KVCommModule commModule;
	protected static final Logger logger = Logger.getLogger(KVStore.class);
	private Socket clientSocket;
	private Set<KVSocketListener> listeners;
	private boolean isRunning = false;
	protected MetadataHandler metadataHandler;
	protected static final int MAX_TRIALS = 3;
	private static final int MAX_WAITING_MS = 3000;
	public Thread connection;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore(String defaultIp, int defaultPort) throws IllegalArgumentException {
		initMetadata(defaultIp, defaultPort);
	}
	public KVStore(Address defaultAdress) throws IllegalArgumentException {
		initMetadata(defaultAdress.getIp(), defaultAdress.getPort());
	}

	protected abstract void initMetadata(String defaultIp, int defaultPort);

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
		connect(n.getAddress());
	}

	protected void connect(Address address) throws IOException {
		logger.debug("Trying to connect to " + address);
		clientSocket = new Socket(address.getIp(), address.getPort());

		commModule = new KVCommModule(clientSocket.getOutputStream(),
				clientSocket.getInputStream());
		listeners = new HashSet<KVSocketListener>();
		addListener(commModule);
		//		setRunning(true);
		//		clientSocket.setSoTimeout(MAX_WAITING_MS);
		connection = new KVStoreConnectionThread();
		connection.start();
	}

	private void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
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

	private synchronized void tearDownConnection() throws IOException {
		//		setRunning(false);

		if (clientSocket != null) {
			commModule.closeStreams();
			clientSocket.close();
			clientSocket = null;
			logger.debug("Connection closed.");
		}
	}

	/**
	 * Initializes and starts the client connection. Loops until the connection
	 * is closed or aborted by the client.
	 */
	public class KVStoreConnectionThread extends Thread {
		public void run() {
			try {
				//				while (isRunning) {
				try {
					TextMessage latestMsg = commModule.receiveMessage();
					for (KVSocketListener listener : listeners) {
						listener.handleNewMessage(latestMsg);
					}
					//					} catch (SocketTimeoutException te) {
					//						// do nothing
				} catch (IOException ioe) {
					if (isRunning) {
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
				//				}
				logger.debug("KVStore stopped");
			} finally {
				//				if (isRunning) {
				disconnect();
				//				}
			}
		}
	}

	@Override
	public abstract KVMessage put(String key, String value) throws IOException,
	InterruptedException, NoSuchAlgorithmException;

	@Override
	public abstract KVMessage get(String key) throws IOException, InterruptedException,
	NoSuchAlgorithmException;

	protected KVMessage sendAndWaitAnswer(KVMessage request) throws IOException, InterruptedException {
		commModule.sendKVMessage(request);

		int t = 0;
		while (t < MAX_WAITING_MS && commModule.latestIsNull()) {
			Thread.sleep(100);
			t += 100;
			if (t >= MAX_WAITING_MS) {
				return null;
			}
		}
		return commModule.getLatest();
	}

	public void setLoggerLevel(Level wantedLevel) {
		logger.setLevel(wantedLevel);
		commModule.setLoggerLevel(wantedLevel);
	}
}
