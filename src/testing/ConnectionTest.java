package testing;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.log4j.Level;

import client.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;


public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() {
		try {
			new LogSetup("testing/test.log", Level.ERROR);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("127.0.0.1", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		if (ex!= null) {
			ex.printStackTrace();
		}
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}

		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}

		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}

