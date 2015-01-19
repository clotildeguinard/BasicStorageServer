package common.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;

public class KVCommModule extends CommModule implements SocketListener {
	private final static Logger logger = Logger.getLogger(KVCommModule.class);
	private KVMessage latest;

	public KVCommModule(OutputStream output, InputStream input) {
		super(output, input);
	}

	public void sendKVMessage(KVMessage message) throws SocketException, IOException {
		try {
			sendMessage(((KVMessageImpl) message).marshal());
		} catch (SocketException se) {
			logger.error("Message " + message + " could not be sent because of : " + se.getMessage());
			throw(se);
		}
	}

	public KVMessage receiveKVMessage() throws IllegalStateException, IOException {
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

	public void setLoggerLevel(Level wantedLevel) {
		logger.setLevel(wantedLevel);	
	}
}
