package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import app_kvServer.KVServer.ECSSocketLoop;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests2 {

	static {
		try {
			new LogSetup("testing/test.log", Level.DEBUG);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
//		clientSuite.addTestSuite(SimpleECSTest.class);
//		clientSuite.addTestSuite(LatencyTest.class);
		clientSuite.addTestSuite(ManyClientsTest.class);
		return clientSuite;
	}

	
}
