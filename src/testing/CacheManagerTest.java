package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.Test;

import app_kvServer.CacheManager;
import app_kvServer.Storage;
import app_kvServer.cache_strategies.FIFOStrategy;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import junit.framework.TestCase;
import logger.LogSetup;

public class CacheManagerTest extends TestCase {
	
	@Test
	public void testCacheManager() {
		IOException ex = null;
		try {
			CacheManager c = new CacheManager(new FIFOStrategy(3), new Storage("./testing/", "50000"));
			
			KVMessage result = c.put("hello", "everybody");
			assertEquals(StatusType.PUT_SUCCESS, result.getStatus());

			// read in cache
			result = c.get("none");
			assertEquals(StatusType.GET_ERROR, result.getStatus());

			c.put("hello1", "everybody1");
			c.put("hello2", "everybody2");
			c.put("hello3", "everybody3");
			result = c.put("hello4", "everybody4");
			assertEquals(StatusType.PUT_SUCCESS, result.getStatus());
			
			//overwrite record stored in cache
			result = c.put("hello2", "someone2");
			assertEquals(StatusType.PUT_UPDATE, result.getStatus());
			result = c.get("hello2");
			assertEquals(StatusType.GET_SUCCESS, result.getStatus());
			assertEquals("someone2", result.getValue());

			// read in file
			result = c.get("hello");
			assertEquals(StatusType.GET_SUCCESS, result.getStatus());
			assertEquals("everybody", result.getValue());
			
			//overwrite record stored in file
			result = c.put("hello", "someone");
			assertEquals(StatusType.PUT_UPDATE, result.getStatus());
			result = c.get("hello");
			assertEquals(StatusType.GET_SUCCESS, result.getStatus());
			assertEquals("someone", result.getValue());
			
			
		} catch (IOException e) {
			ex = e;
		}
		assertNull(ex);

	}
}
