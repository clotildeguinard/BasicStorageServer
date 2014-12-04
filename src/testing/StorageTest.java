package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.Test;

import app_kvServer.Storage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import junit.framework.TestCase;
import logger.LogSetup;

public class StorageTest extends TestCase {
	
	@Test
	public void testStorage() throws IOException {
		IOException ex = null;
		try {
			new LogSetup("./testing/server.log", Level.ALL);
			Storage s = new Storage("./testing/", "50000");
			
			s.put("hello",  "world");
			s.put("are", "you");
			s.put("christmas", "tree");
			
			KVMessage result = s.get("hello");
			assertEquals(StatusType.GET_SUCCESS, result.getStatus());
			assertEquals("world", result.getValue());
			
			result = s.get("none");
			assertEquals(StatusType.GET_ERROR, result.getStatus());

			s.put("hello", "cheese");
			s.put("foo", "bar");
			
			result = s.get("hello");
			assertEquals(StatusType.GET_SUCCESS, result.getStatus());
			assertEquals("cheese", result.getValue());

			result = s.get("are");
			assertEquals(StatusType.GET_SUCCESS, result.getStatus());
					
		} catch (IOException e) {
			ex = e;
		}
		assertNull(ex);
	}

	
}
