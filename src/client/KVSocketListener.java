package client;


public interface KVSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleNewMessage(String msg);
	
	public void handleStatus(SocketStatus status);
}