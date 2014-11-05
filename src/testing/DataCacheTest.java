package testing;

import org.junit.Test;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import app_kvServer.DataCache;
import app_kvServer.cache_strategies.FIFOStrategy;
import app_kvServer.cache_strategies.LFUStrategy;
import app_kvServer.cache_strategies.LRUStrategy;
import junit.framework.TestCase;

public class DataCacheTest extends TestCase {
	

	
	@Test
	public void testLRU() {
		KVMessage answer;
		DataCache cache = new LRUStrategy(3);
		cache.put("first", "kv");
		cache.put("foo", "bar");
		cache.put("hello", "zebra");
		cache.put("cat", "dog");
		assertEquals("cat - dog / hello - zebra / foo - bar / ", cache.toString());
		
		cache.put("foo", "bar");
		assertEquals("foo - bar / cat - dog / hello - zebra / ", cache.toString());
		
		answer = cache.get("cat");
		assertEquals(StatusType.GET_SUCCESS, answer.getStatus());
		assertEquals("cat", answer.getKey());
		assertEquals("dog", answer.getValue());
		assertEquals("cat - dog / foo - bar / hello - zebra / ", cache.toString());
		
		answer = cache.get("cattt");
		assertNull(answer);
		assertEquals("cat - dog / foo - bar / hello - zebra / ", cache.toString());
		
		cache.put("cat", "mouse");
		cache.put("cat", "mouse");
		cache.put("hi", "new");
		assertEquals("hi - new / cat - mouse / foo - bar / ", cache.toString());
	}

	@Test
	public void testLFU() {
		KVMessage answer;
		DataCache cache = new LFUStrategy(3);
		cache.put("first", "kv");
		cache.put("foo", "bar");
		cache.put("hello", "zebra");
		cache.put("cat", "dog");
		assertEquals("hello - zebra - 0 / foo - bar - 0 / cat - dog - 0 / ", cache.toString());
		
		answer = cache.get("cattt");
		assertNull(answer);
		assertEquals("hello - zebra - 0 / foo - bar - 0 / cat - dog - 0 / ", cache.toString());
		
		cache.put("foo", "bar");
		assertEquals("hello - zebra - 0 / cat - dog - 0 / foo - bar - 0 / ", cache.toString());
		
		answer = cache.get("cat");
		assertEquals(StatusType.GET_SUCCESS, answer.getStatus());
		assertEquals("cat", answer.getKey());
		assertEquals("dog", answer.getValue());
		
		cache.get("cat");
		cache.get("cat");
		cache.get("hello");
		assertEquals("foo - bar - 0 / cat - dog - 3 / hello - zebra - 1 / ", cache.toString());
		
		cache.put("cat", "mouse");
		cache.put("hi", "new");
		assertEquals("cat - mouse - 0 / hello - zebra - 1 / hi - new - 0 / ", cache.toString());
	}
	
	@Test
	public void testFIFO() {
		KVMessage answer;
		DataCache cache = new FIFOStrategy(3);
		cache.put("first", "kv");
		cache.put("foo", "bar");
		cache.put("hello", "zebra");
		cache.put("cat", "dog");
		assertEquals("cat - dog / hello - zebra / foo - bar / ", cache.toString());
		
		cache.put("foo", "bar");
		assertEquals("foo - bar / cat - dog / hello - zebra / ", cache.toString());
		
		answer = cache.get("cat");
		assertEquals(StatusType.GET_SUCCESS, answer.getStatus());
		assertEquals("cat", answer.getKey());
		assertEquals("dog", answer.getValue());
		assertEquals("foo - bar / cat - dog / hello - zebra / ", cache.toString());
		
		answer = cache.get("cattt");
		assertNull(answer);
		assertEquals("foo - bar / cat - dog / hello - zebra / ", cache.toString());
		
		cache.put("cat", "mouse");
		cache.put("cat", "mouse");
		cache.put("hi", "new");
		assertEquals("hi - new / cat - mouse / foo - bar / ", cache.toString());
	}
}
