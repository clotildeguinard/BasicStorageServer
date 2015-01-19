package common.communication;

import common.messages.TextMessage;


public interface SocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleNewMessage(TextMessage msg);
	
	public void handleStatus(SocketStatus status);
}