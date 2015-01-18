package common.messages;

import org.apache.log4j.Logger;

import common.messages.KVAdminMessage.StatusType;

public class KVMessageImpl implements KVMessage {
	public String key;
	public String value;
	public static StatusType statusType;
	private final static Logger logger = Logger.getLogger("KVMessageImpl");

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


	/**
	 * @return the status type that is associated with this message, 
	 * 		null if not value is associated.
	 */
	@Override
	public StatusType getStatus() {
		return statusType;
	}

	public TextMessage marshal() {
		StringBuilder q1=new StringBuilder();

		q1.append("<kvmessage>");
		q1.append("<status>"+ getStatus().toString().toLowerCase() + "</status>");
		q1.append ("<key>"+getKey()+"</key>");
		q1.append ("<value>"+getValue()+"</value>");

		q1.append("</kvmessage>");
		String s=q1.toString();

		return new TextMessage(s);

	}

	public static KVMessage unmarshal(TextMessage text) {

		String xml = text.getMsg();
		try {
			String key = unmarshalParameter(xml, "key");
			String value = unmarshalParameter(xml, "value");
			String status = unmarshalParameter(xml, "status");

			if (status != null) {
				try {
					StatusType statusType = StatusType.valueOf(status.toUpperCase());
					return new KVMessageImpl(key, value, statusType);
				} catch (IllegalArgumentException e) {
					try {
						common.messages.KVAdminMessage.StatusType.valueOf(status.toUpperCase());
						throw new IllegalStateException("Connected to ECS instead of a client or an other node !");

					} catch (IllegalArgumentException ex) {

					}
				}
			}
		} catch (IndexOutOfBoundsException | IllegalArgumentException e) {
			logger.warn("Impossible to unmarshal received message : " + xml);
		}
		return null;
	}

	private static String unmarshalParameter(String xml, String parameter) throws IndexOutOfBoundsException {
			String start_tag = "<" + parameter + ">";
			String end_tag = "</" + parameter + ">";
			int beginIndex = xml.indexOf(start_tag)
					+ parameter.length() + 2;
			int endIndex = xml.indexOf(end_tag);
			return xml.substring(beginIndex, endIndex);
	}

	@Override
	public String toString() {
		return getStatus() + " " + getKey() + " " + getValue();
	}

}
