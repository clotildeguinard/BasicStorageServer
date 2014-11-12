package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import client.KVSocketListener.SocketStatus;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;

public class KVCommModule implements KVSocketListener {
	private static Logger logger = Logger.getRootLogger();
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private OutputStream output;
 	private InputStream input;
 	private KVMessage latest;

	public KVCommModule(OutputStream output, InputStream input) {
		this.output = output;
		this.input = input;
	}

	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @param output 
	 * @throws IOException some I/O error regarding the output stream 
	 */
	protected void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
    }

	protected TextMessage receiveMessage() throws IOException {
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
		return msg;
    }

	public void sendKVMessage(KVMessage message) throws IOException {
		logger.info("Send :\t '" + message + "'");
		TextMessage xmlText = ((KVMessageImpl) message).marshal();
		sendMessage(xmlText);
	}



	public KVMessage receiveKVMessage() throws IOException {
		TextMessage xmlText = receiveMessage();
		KVMessage received = KVMessageImpl.unmarshal(xmlText);
		logger.info("Receive :\t '" + received + "'");
		return received;
	}

	public void closeStreams() throws IOException {
		output.close();
		input.close();
	}

	@Override
	public void handleNewMessage(TextMessage msg) {
		latest = KVMessageImpl.unmarshal(msg);	
		logger.info("New received message : " + latest);
	}

	@Override
	public void handleStatus(SocketStatus status) {}

	public KVMessage getLatest() {
		try {
		return latest;
		} finally {
			latest = null;
		}
	}

	public boolean latestIsNull() {
		return latest == null;
	}
}
