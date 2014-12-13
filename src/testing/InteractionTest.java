package testing;

import org.apache.log4j.Level;
import org.junit.Test;

import app_kvClient.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			new LogSetup("./testing/test.log", Level.ALL);
//			kvClient.connect();
		} catch (Exception e) {
		}
	}

//	public void tearDown() {
//		kvClient.disconnect();
//	}
	
	
	@Test
	public void testPut() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		System.out.println(response);
		if (ex!= null) {
			ex.printStackTrace();
		}
		assertNull(ex);
		assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
		System.out.println("-----------------------------------------------------------------------");
	}
	
//	@Test
//	public void testPutDisconnected() {
//		kvClient.disconnect();
//		String key = "foo";
//		String value = "bar";
//		Exception ex = null;
//
//		try {
//			kvClient.put(key, value);
//		} catch (Exception e) {
//			ex = e;
//		}
//
//		assertNotNull(ex);
//		System.out.println("-----------------------------------------------------------------------");
//	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}
		if (ex!= null) {
			ex.printStackTrace();
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
		System.out.println("-----------------------------------------------------------------------");
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}
		if (ex!= null) {
			ex.printStackTrace();
		}
		assertNull(ex);
		assertEquals(StatusType.DELETE_SUCCESS, response.getStatus());
		System.out.println("-----------------------------------------------------------------------");
	}
		
	@Test
	public void testGet() {
		String key = "foo1";
		String value = "bar1";
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		if (ex!= null) {
			ex.printStackTrace();
		}
		assertNull(ex);
		assertEquals("bar1", response.getValue());
		System.out.println("-----------------------------------------------------------------------");
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			e.printStackTrace();
			ex = e;
		}
if (ex!= null) {
	ex.printStackTrace();
}
		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
		System.out.println("-----------------------------------------------------------------------");
	}
	


}
