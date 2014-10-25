package app_kvClient;

import java.io.IOException;

// NADIA
public class KVClient extends Thread {
	private boolean connected = false;

	public void run() {
	try {
		/* create socket, connect to server and init streams */
		// call Clotilde's function connect() via the KVCommInterface
		while (connected) { 
			//communicate 
			// extend the API of milestone 1 to handle PUT and GET
			// call Clotilde's functions put() and get() via the KVCommInterface
		}
//	}
//		catch (IOException ex) {
//			// handle gracefully
	} finally {
		/* close streams and socket */
		// call Clotilde's function disconnect() via the KVCommInterface
	}
	}
}
