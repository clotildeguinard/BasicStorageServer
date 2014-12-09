package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests2 {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.DEBUG);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ECSTest.class);
//		clientSuite.addTestSuite(BenchmarkTest.class);
		return clientSuite;
	}
	
}
