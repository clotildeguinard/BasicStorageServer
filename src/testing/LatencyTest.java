package testing;

import java.io.IOException;

import org.junit.Test;

import app_kvClient.KVClient;
import app_kvEcs.ECSInterface;
import app_kvServer.KVServer;
import junit.framework.TestCase;


public class LatencyTest extends TestCase {
	private static ECSInterface ecs;
	private static KVClient kvclient;

	public void before(int cacheSize, int nbNodes) throws IOException {	
		for (int i = 0; i < nbNodes; i ++) {
			new KVServer(50000 + i);
		}

		ecs = new ECSInterface("./testing/ecs.config.txt");
		ecs.handleCommand("init "+ nbNodes + " " + cacheSize + " FIFO");
		ecs.handleCommand("start");

		kvclient = new KVClient();
	}

	private void after() {
		if (kvclient != null) {
			kvclient.handleCommand("quit");
		}
		if (ecs != null) {
			ecs.handleCommand("quit");
		}
	}

	@Test
	public void test1() {
		task(50, 25, 3);
//      task(50, 25, 1);
//		task(50, 50, 1);
		
//		task(50, 5, 3);
//		task(50, 5, 3);
//		task(50, 5, 3);
	}

	private void task(int nbRequests, int cacheSize, int nbNodes) {
		Exception ex = null;

		try {

			before(cacheSize, nbNodes);
			
			long startTime = System.currentTimeMillis();

			// Task1

			for (int i = 0; i < nbRequests; i++) {
				kvclient.handleCommand("put foo" + i + " " + "bar" + i);
			}
			long time1 = System.currentTimeMillis();
			System.out.println("Task with " + nbRequests + " requests, " + nbNodes + " nodes and cacheSize " + cacheSize + " took : " + (time1 - startTime) + " ms");
//
//			// Task2
//
//			ecs.handleCommand("addnode " + cacheSize + " FIFO");
//			long time2 = System.currentTimeMillis();
//			System.out.println("Task2 : " + (time2 - time1) + " ms");
//
//
//			// Task3
//
//			ecs.handleCommand("removenode");
//			long time3 = System.currentTimeMillis();
//			System.out.println("Task3 : " + (time3 - time2) + " ms");


			System.out.println("BenchmarkTest1 : " + (System.currentTimeMillis() - startTime) + " ms");
		} catch (Exception e) {
			ex = e;
		} finally {
			after();
		}

		if (ex!= null) {
			ex.printStackTrace();
		}
		assertNull(ex);
	}



}

