package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			KVServer kvserver = new KVServer(50000);
			kvserver.initKVServer("node0;127.0.0.1;50000;00000000000000000000000000000000;ffffffffffffffffffffffffffffffff", 10, "FIFO");
			kvserver.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(DataCacheTest.class); 
		clientSuite.addTestSuite(StorageTest.class); 
		clientSuite.addTestSuite(CacheManagerTest.class); 
		return clientSuite;
	}
	
}
