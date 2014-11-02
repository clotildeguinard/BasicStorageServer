package common.messages;

public class KVMessageImpl implements KVMessage {
	public String key;
	public String value;
	public StatusType statusType;

	public KVMessageImpl(String key, String value, StatusType statusType) {
		this.key = key;
		this.value = value;
		this.statusType = statusType;
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	@Override
	public String getKey(){
		return key;
	}
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	@Override
	public String getValue(){
		return value;
	}

	public byte[] serialize() {
		//TODO
		return null;
	}

	
	public String marshal() {
		//TODO
		return null;
	}

	/**
	 * @return the status type that is associated with this message, 
	 * 		null if not value is associated.
	 */
	@Override
	public StatusType getStatus() {
		return statusType;
	}
}
