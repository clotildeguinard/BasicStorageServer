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

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

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
		try {
			clientSocket.close();
			output.close();
			input.close();
			setRunning(false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		sendMessage(protocText);
	}

	public KVMessage receiveKVMessage() throws IOException {
		//TODO
		TextMessage protocText = receiveMessage();
		// protocol: transform TextMessage into KVMessage
		KVMessage received = null;
		return received;
	}

	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	private void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
//		logger.info("Send message:\t '" + msg.getMsg() + "'");
    }
	

	private TextMessage receiveMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
//		logger.info("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
    }
}
