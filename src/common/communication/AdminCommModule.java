package common.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;

import org.apache.log4j.Logger;

import common.messages.AdminMessage;
import common.messages.AdminMessageImpl;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;

public class AdminCommModule extends CommModule implements SocketListener {
	private final static Logger logger = Logger.getLogger(AdminCommModule.class);
 	private AdminMessage latest;
 	private TextMessage latestXmlTxt;

	public AdminCommModule(OutputStream output, InputStream input) {
		super(output, input);
	}

	
	public synchronized void sendKVAdminMessage(AdminMessage message) throws IOException {
			try {
				sendMessage(((AdminMessageImpl) message).marshal());
			} catch (IOException e) {
				logger.error("Message " + message + " could not be sent because of : " + e.getMessage());
				throw(e);
			}
	}
	
	public TextMessage getLatestXmlTxt() {
		return latestXmlTxt;
	}

	public AdminMessage receiveKVAdminMessage() throws IllegalStateException, IOException {
		latestXmlTxt = receiveMessage();
		return AdminMessageImpl.unmarshal(latestXmlTxt);
	}

	@Override
	public void handleNewMessage(TextMessage msg) {
		latest = AdminMessageImpl.unmarshal(msg);	
		logger.debug("New received message : " + latest);
	}

	@Override
	public void handleStatus(SocketStatus status) {}

	public AdminMessage getLatest() {
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
