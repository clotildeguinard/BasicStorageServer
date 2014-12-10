package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.Test;

import app_kvClient.KVClient;
import app_kvEcs.ECSInterface;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import logger.LogSetup;


public class ManyClientsTest extends TestCase {
	private static ECSInterface ecs;
	private static KVClient kvclient1;
	private static KVClient kvclient2;
	private static KVClient kvclient3;

	static {
		try {
			new LogSetup("testing/test.log", Level.DEBUG);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public void before() {
		//should be removed if ssh could be used in initKVServer(...)
		/////////////////

		KVServer kvserver = new KVServer(50000);
		new Thread(kvserver.new ECSSocketLoop()).start();

		kvserver = new KVServer(50001);
		new Thread(kvserver.new ECSSocketLoop()).start();

		kvserver = new KVServer(50002);
		new Thread(kvserver.new ECSSocketLoop()).start();

		kvserver = new KVServer(50003);
		new Thread(kvserver.new ECSSocketLoop()).start();
		////////////////


		ecs = new ECSInterface("./testing/ecs.config.txt");
		ecs.handleCommand("init 2 5 LRU");
		ecs.handleCommand("start");

		kvclient1 = new KVClient();
		kvclient2 = new KVClient();
		kvclient3 = new KVClient();
	}


	@Test
	public void test1() {
		before();
		Exception ex = null;

		try {
			long startTime = System.currentTimeMillis();

			// Task1
			Thread t1 = clientThread(kvclient1);
			Thread t2 = clientThread(kvclient2);
			Thread t3 = clientThread(kvclient3);
			t3.start();
			t1.start();
			t2.start();
			
			t3.join();
			t1.join();
			t2.join();


			long time1 = System.currentTimeMillis();
			System.out.println("Task1 : " + (time1 - startTime) + " ms");

			// Task2

			ecs.handleCommand("addnode 5 FIFO");
			long time2 = System.currentTimeMillis();
			System.out.println("Task2 : " + (time2 - time1) + " ms");


			// Task3

			ecs.handleCommand("removenode");
			long time3 = System.currentTimeMillis();
			System.out.println("Task3 : " + (time3 - time2) + " ms");


			System.out.println("BenchmarkTest1 : " + (System.currentTimeMillis() - startTime) + " ms");
		} catch (Exception e) {
			ex = e;
		} finally {
			kvclient1.handleCommand("quit");
			ecs.handleCommand("shutdown");
		}

		if (ex!= null) {
			ex.printStackTrace();
		}
		assertNull(ex);
	}
	
	private Thread clientThread(final KVClient client) {
		return new Thread() {
			public void run(){
				for (int i=0; i<30; i++) {
					client.handleCommand("put foo" + i + " " + "bar" + i);
				}
			}
		};
	}



}

