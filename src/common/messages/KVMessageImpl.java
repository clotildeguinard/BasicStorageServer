package common.messages;



public class KVMessageImpl implements KVMessage {
	public String key;
	public String value;
	public static StatusType statusType;

	public KVMessageImpl(String key, String value, StatusType statusType) {
		this.key = key;
		this.value = value;
		this.statusType = statusType;
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
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

<<<<<<< HEAD
	public byte[] serialize() {
		return null;
		
	
	}

=======
>>>>>>> FETCH_HEAD
	
	public TextMessage marshal() {
		//TODO
		
		String s;

			StringBuilder q1=new StringBuilder();
			
			q1.append("<kvmessage>");
		    q1.append("<status>"+ getStatus()+ "</status>");
		    q1.append ("<key>"+getKey()+"</key>");
		    q1.append ("<value>"+getValue()+"</value>");
		    
		   q1.append("</kvmessage>");
		    s=q1.toString();
		   
		  return new TextMessage(s);
		  
		}
		   
<<<<<<< HEAD
	public static KVMessage unmarshal(TextMessage text) {
		
		getStatus();
		
		getKey();
		
		getValue();
		
		return null;
			
	}
=======

>>>>>>> FETCH_HEAD

	/**
	 * @return the status type that is associated with this message, 
	 * 		null if not value is associated.
	 */
	@Override
	public StatusType getStatus() {
		return statusType;
	}
}
