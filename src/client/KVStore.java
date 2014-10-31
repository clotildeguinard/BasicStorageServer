package client;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

import common.messages.KVMessage;
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
		this.commModule = new KVCommModule();
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

	@Override
	public void disconnect() {
		// copy and adapt from previous milestone
//		logger.info("try to close connection ...");
		
		try {
			tearDownConnection();
//			for(ClientSocketListener listener : listeners) {
//				listener.handleStatus(SocketStatus.DISCONNECTED);
//			}
		} catch (IOException ioe) {
//			logger.error("Unable to close connection!");
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
	public KVMessage put(String key, String value) throws Exception {
		KVMessage msg = null;
		return handleRequest(msg);
	}

	@Override
	public KVMessage get(String key) throws Exception {
		KVMessage msg = null;
		return handleRequest(msg);
	}

	private KVMessage handleRequest(KVMessage request) throws IOException {
		sendKVMessage(request);
		return receiveKVMessage();
	}

	/* ex: http protocol 1.0
	GET /page.html HTTP/1.0
	Host: example.com
	Referer: http://example.com/
	User-Agent: CERN-LineMode/2.15 libwww/2.17b3 */

	public void sendKVMessage(KVMessage message) throws IOException {
		//TODO
		// protocol: transform KVMessage into String
		TextMessage protocText = new TextMessage("");
		commModule.sendMessage(protocText, output);
	}

	public KVMessage receiveKVMessage() throws IOException {
		//TODO
		TextMessage protocText = commModule.receiveMessage(input);
		// protocol: transform TextMessage into KVMessage
		KVMessage received = null;
		return received;
	}

}
