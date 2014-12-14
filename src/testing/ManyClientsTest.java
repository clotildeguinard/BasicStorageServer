package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.Test;

import app_kvClient.KVClient;
import app_kvEcs.ECSInterface;
import app_kvServer.KVServer;
import app_kvServer.KVServer.ECSSocketLoop;
import junit.framework.TestCase;
import logger.LogSetup;


public class ManyClientsTest extends TestCase {
	private static ECSInterface ecs;
	private static KVClient[] kvclients;
	private static Thread[] clientThreads;

	static {
		try {
			new LogSetup("testing/test.log", Level.WARN);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private void before(int nbConcurrentClients) {
		KVServer kvServer = new KVServer(50000);
		new Thread(kvServer.new ECSSocketLoop()).start();

		kvServer = new KVServer(50001);
		new Thread(kvServer.new ECSSocketLoop()).start();

		kvServer = new KVServer(50002);
		new Thread(kvServer.new ECSSocketLoop()).start();

		kvServer = new KVServer(50003);
		new Thread(kvServer.new ECSSocketLoop()).start();

		ecs = new ECSInterface("./testing/ecs.config.txt");
		ecs.handleCommand("init 4 5 LRU");
		ecs.handleCommand("start");

		kvclients = new KVClient[nbConcurrentClients];
		for (int i = 0; i < nbConcurrentClients ; i ++) {
			kvclients[i] = new KVClient();
		}
	}

	private void after() {
		ecs.handleCommand("shutdown");
		ecs.handleCommand("quit");
	}


	@Test
	public void test1() {
		task(3);
	}

	private void task(int nbConcurrentClients) {
		before(nbConcurrentClients);
		Exception ex = null;

		try {
			long startTime = System.currentTimeMillis();

			// Task1
			clientThreads = new Thread[nbConcurrentClients];
			for (int i = 0; i < nbConcurrentClients ; i ++) {
				clientThreads[i] = clientThread(kvclients[i], i);
			}
			for (Thread t : clientThreads) {
				t.start();
			}
			for (Thread t : clientThreads) {
				t.join();
			}


			long time1 = System.currentTimeMillis();

			System.out.println("----------------------------------------------------------------------");
			System.out.println("Task with " + nbConcurrentClients + " concurrent clients : " + (time1 - startTime) + " ms");
			System.out.println("----------------------------------------------------------------------");

		} catch (Exception e) {
			ex = e;
			ex.printStackTrace();
		} finally {
			after();
		}

		assertNull(ex);
	}

	private Thread clientThread(final KVClient client, final int index) {
		return new Thread() {
			public void run(){
				for (int i=0; i<30; i++) {
					client.handleCommand("put foo" + (30*index + i) + " " + "bar" + (30*index + i));
				}
				client.handleCommand("quit");
			}
		};
	}



}

