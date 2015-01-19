package testing;

import org.junit.Test;

import app_kvEcs.ECSInterface;
import app_kvServer.KVServer;
import junit.framework.TestCase;


public class SimpleECSTest extends TestCase {

	static {
		new KVServer(50000);
		new KVServer(50001);
		new KVServer(50002);
	}

	@Test
	public void testNodeCycle() {

		Exception ex = null;

		ECSInterface ecs = new ECSInterface("./testing/ecs.config.txt");

		try {
			long startTime = System.currentTimeMillis();
			ecs.handleCommand("init 3 5 LRU");
			ecs.handleCommand("start");
			ecs.handleCommand("start");
			ecs.handleCommand("stop");
			ecs.handleCommand("stop");
			ecs.handleCommand("start");
			ecs.handleCommand("shutdown");
			ecs.handleCommand("quit");
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

