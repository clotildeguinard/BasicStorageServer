package common.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;

public class KVCommModule extends CommModule implements KVSocketListener {
	private final static Logger logger = Logger.getLogger(KVCommModule.class);
	private KVMessage latest;

	public KVCommModule(OutputStream output, InputStream input) {
		super(output, input);
	}

	public void sendKVMessage(KVMessage message) throws IOException {
		sendMessage(((KVMessageImpl) message).marshal());
	}

	public KVMessage receiveKVMessage() throws IOException {
		TextMessage xmlText = receiveMessage();
		return KVMessageImpl.unmarshal(xmlText);
	}

	@Override
	public void handleNewMessage(TextMessage msg) {
		latest = KVMessageImpl.unmarshal(msg);	
		logger.debug("New received message : " + latest);
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
