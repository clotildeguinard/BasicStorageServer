package client;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import client.KVSocketListener.SocketStatus;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;

/**
 * send requests to the server, requires call to the defined communication protocol (encode, decode)
 * @author Clotilde
 *
 */
public class KVStore extends Thread implements KVCommInterface {
	private final String address;
	private final int port;
	private KVCommModule commModule;

	private Logger logger = Logger.getRootLogger();

	private Socket clientSocket;
	private Set<KVSocketListener> listeners;
	private boolean running = false;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	public void addListener(KVSocketListener listener){
		listeners.add(listener);
	}
	
	@Override
	public void connect() throws UnknownHostException, IOException {
		clientSocket = new Socket(address, port);
		commModule = new KVCommModule(clientSocket.getOutputStream(), clientSocket.getInputStream());
		listeners = new HashSet<KVSocketListener>();
		addListener(commModule);
		setRunning(true);
		start();
	}

	private void setRunning(boolean isRunning) {
		running = isRunning;
	}
	public boolean isRunning() {
		return running;
	}
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			
			while(isRunning()) {
				try {
					TextMessage latestMsg = commModule.receiveMessage();
					for(KVSocketListener listener : listeners) {
						listener.handleNewMessage(latestMsg);
					}
				} catch (IOException ioe) {
					if(isRunning()) {
						logger.error("Connection lost!");
						try {
							tearDownConnection();
							for(KVSocketListener listener : listeners) {
								listener.handleStatus(
										SocketStatus.CONNECTION_LOST);
							}
						} catch (IOException e) {
							logger.error("Unable to close connection!");
						}
					}
				}				
			}
		} finally {
			if(isRunning()) {
				disconnect();
			}
		}
	}

	@Override
	public void disconnect() {
		//TODO synchronized?????
			logger.info("try to close connection ...");
			
			try {
				tearDownConnection();
				if (listeners != null) {
					for(KVSocketListener listener : listeners) {
						listener.handleStatus(SocketStatus.DISCONNECTED);
					}
				}
			} catch (IOException ioe) {
				logger.error("Unable to close connection!");
			}
	}
	
	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			commModule.closeStreams();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws IOException, InterruptedException {
		KVMessage msg = new KVMessageImpl(key, value, common.messages.KVMessage.StatusType.PUT);
		commModule.sendKVMessage(msg);
		
		int i = 0;
		while (i<5 && commModule.latestIsNull()) {
			Thread.sleep(50);
			i++;
		}
		if (i == 5) {
			return null;
		}
		return commModule.getLatest();
	}

	/**
	 * returns kvmessage, or null if no kvmessage arrives within a given time interval
	 */
	@Override
	public KVMessage get(String key) throws IOException, InterruptedException {
		KVMessage msg = new KVMessageImpl(key, null, common.messages.KVMessage.StatusType.GET);
		commModule.sendKVMessage(msg);
		
		int i = 0;
		while (i<5 && commModule.latestIsNull()) {
			Thread.sleep(50);
			i++;
		}
		if (i == 5) {
			return null;
		}
		return commModule.getLatest();
	}


}
