package testing;

import org.junit.Test;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;
import junit.framework.TestCase;

public class MarshalTest extends TestCase {
	final TextMessage text = new TextMessage("<kvmessage>" + "<status>get</status>" + 
			"<key>cat</key>" + "<value>dog</value>" + 
			"</kvmessage>");
	final KVMessageImpl kvMessage = new KVMessageImpl("cat", "dog", StatusType.GET);
	

	
	@Test
	public void testMarshal() {
		TextMessage marshalledKVM = kvMessage.marshal();
		assertEquals(text.getMsg(), marshalledKVM.getMsg());
	}
	
	@Test
	public void testUnmarshal() {
		KVMessageImpl unmarshalledText = KVMessageImpl.unmarshall(text);
		assertEquals(kvMessage.getKey(), unmarshalledText.getKey());
		assertEquals(kvMessage.getValue(), unmarshalledText.getValue());
		assertEquals(kvMessage.getStatus(), unmarshalledText.getStatus());
	}

	
}
