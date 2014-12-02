package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.TextMessage;

public class KVAdminCommModule extends CommModule implements KVSocketListener {
	private Logger logger = Logger.getLogger(getClass().getSimpleName());
 	private KVAdminMessage latest;

	public KVAdminCommModule(OutputStream output, InputStream input) {
		super(output, input);
	}

	
	public void sendKVAdminMessage(KVAdminMessage message) throws IOException {
		logger.debug("Send :\t '" + message + "'");
		TextMessage xmlText = ((KVAdminMessageImpl) message).marshal();
		sendMessage(xmlText);
	}


	public KVAdminMessage receiveKVAdminMessage() throws IOException {
		TextMessage xmlText = receiveMessage();
		KVAdminMessage received = KVAdminMessageImpl.unmarshal(xmlText);
		logger.debug("Receive :\t '" + received + "'");
		return received;
	}

	@Override
	public void handleNewMessage(TextMessage msg) {
		latest = KVAdminMessageImpl.unmarshal(msg);	
		logger.info("New received message : " + latest);
	}

	@Override
	public void handleStatus(SocketStatus status) {}

	public KVAdminMessage getLatest() {
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
