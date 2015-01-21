package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import app_kvEcs.ECSInterface;
import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {
	static KVServer kvserver0;
	static KVServer kvserver1;
	static KVServer kvserver2;
	static ECSInterface ecsInterface;

	static {
		try {
			new LogSetup("testing/test.log", Level.DEBUG);

			kvserver0 = new KVServer(50000);
			kvserver1 = new KVServer(50001);
			kvserver2 = new KVServer(50002);

			ecsInterface = new ECSInterface("./testing/ecs.config.txt");
			ecsInterface.handleCommand("init 3 5 LRU");
			ecsInterface.handleCommand("start");

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

	public void tearDown() {
		ecsInterface.handleCommand("quit");
	}


}
