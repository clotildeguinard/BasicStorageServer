package common.messages;


public interface KVAdminMessage {
	
	public enum StatusType {
		LOCK_WRITE,
		UNLOCK_WRITE,
		START,
		STOP,
		SHUTDOWN,
		UPDATE_METADATA,
		MOVE_DATA
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
	public String toString();
	
}


