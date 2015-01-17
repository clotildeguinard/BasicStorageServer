package testing;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.log4j.Level;

import app_kvApi.KVStore;
import app_kvApi.KVStoreClient;
import junit.framework.TestCase;
import logger.LogSetup;


public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() {
//		try {
//			new LogSetup("testing/test.log", Level.DEBUG);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
		
		Exception ex1 = null;
		
		KVStore kvClient = new KVStoreClient("127.0.0.1", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex1 = e;
			ex1.printStackTrace();
		}
		
		assertNull(ex1);
	}
	
	
//	public void testUnknownHost() {
//		Exception ex = null;
//		KVStore kvClient = new KVStoreClient("unknown", 50000);
//		
//		try {
//			kvClient.connect();
//		} catch (Exception e) {
//			ex = e; 
//		}
//
//		assertTrue(ex instanceof UnknownHostException);
//	}
//	
//	
//	public void testIllegalPort() {
//		Exception ex = null;
//		KVStore kvClient = new KVStoreClient("localhost", 123456789);
//		
//		try {
//			kvClient.connect();
//		} catch (Exception e) {
//			ex = e; 
//		}
//
//		assertTrue(ex instanceof IllegalArgumentException);
//	}
	
	

	
}

