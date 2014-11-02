package client;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

import client.KVSocketListener.SocketStatus;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;

/**
 * send requests to the server, requires call to the defined communication protocol (encode, decode)
 * @author Clotilde
 *
 */
public class KVStore implements KVCommInterface {
	private final String address;
	private final int port;
	private KVCommModule commModule;

	private Socket clientSocket;
	private Set<KVSocketListener> listeners;
	private boolean running = false;
	private OutputStream output;
 	private InputStream input;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.commModule = new KVCommModule(output, input);
	}
	
	@Override
	public void connect() throws Exception {
		clientSocket = new Socket(address, port);
		input = clientSocket.getInputStream();
		output = clientSocket.getOutputStream();
		listeners = new HashSet<KVSocketListener>();
		setRunning(true);
	}

	private void setRunning(boolean isRunning) {
		running = isRunning;
	}
	public boolean isRunning() {
		return running;
	}

	@Override
	public void disconnect() {
		//TODO synchronized?????
//			logger.info("try to close connection ...");
			
			try {
				tearDownConnection();
				for(KVSocketListener listener : listeners) {
					listener.handleStatus(SocketStatus.DISCONNECTED);
				}
			} catch (IOException ioe) {
//				logger.error("Unable to close connection!");
			}
	}
	
	private void tearDownConnection() throws IOException {
		setRunning(false);
//		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
//			logger.info("connection closed!");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws IOException {
		KVMessage msg = new KVMessageImpl(key, value, common.messages.KVMessage.StatusType.PUT);
		return handleRequest(msg);
	}

	@Override
	public KVMessage get(String key) throws IOException {
		KVMessage msg = new KVMessageImpl(key, null, common.messages.KVMessage.StatusType.GET);
		return handleRequest(msg);
	}

	private KVMessage handleRequest(KVMessage request) throws IOException {
		commModule.sendKVMessage(request);
		return commModule.receiveKVMessage();
	}


}
