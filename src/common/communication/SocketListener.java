package common.communication;

import common.messages.TextMessage;



public interface SocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleStatus(SocketStatus status);

	void handleNewMessage(TextMessage msg);
}