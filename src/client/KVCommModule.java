package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;

public class KVCommModule extends CommModule implements KVSocketListener {
	private Logger logger = Logger.getLogger(getClass().getSimpleName());
	private KVMessage latest;

	public KVCommModule(OutputStream output, InputStream input) {
		super(output, input);
	}

	public void sendKVMessage(KVMessage message) throws IOException {
		logger.debug("Send :\t '" + message + "'");
		TextMessage xmlText = ((KVMessageImpl) message).marshal();
		sendMessage(xmlText);
	}

	public KVMessage receiveKVMessage() throws IOException {
		TextMessage xmlText = receiveMessage();
		KVMessage received = KVMessageImpl.unmarshal(xmlText);
		logger.debug("Receive :\t '" + received + "'");
		return received;
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
