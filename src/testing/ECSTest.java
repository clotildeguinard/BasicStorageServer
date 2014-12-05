package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

import app_kvEcs.ECSInterface;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import logger.LogSetup;


public class ECSTest extends TestCase {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	@Test
	public void testNodeCycle() {

		Exception ex = null;

		ECSInterface ecs = new ECSInterface("./testing/ecs.config.txt");

		try {
			long startTime = System.currentTimeMillis();
			ecs.handleCommand("init 1 5 LRU");
			ecs.handleCommand("start");
			ecs.handleCommand("start");
			ecs.handleCommand("stop");
			ecs.handleCommand("stop");
			ecs.handleCommand("start");
			ecs.handleCommand("shutdown");
			System.out.println("ECSTest : " + (System.currentTimeMillis() - startTime) + " ms");
		} catch (Exception e) {
			ex = e;
		}	
		if (ex!= null) {
			ex.printStackTrace();
		}
		assertNull(ex);
	}

}

