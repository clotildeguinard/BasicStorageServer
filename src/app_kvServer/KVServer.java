package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVStore;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.StatusType;
import common.messages.KVAdminMessageImpl;
import common.metadata.MetadataHandler;
import app_kvServer.cache_strategies.FIFOStrategy;
import app_kvServer.cache_strategies.LFUStrategy;
import app_kvServer.cache_strategies.LRUStrategy;
import app_kvServer.cache_strategies.Pair;


public class KVServer implements Runnable {
	private final String storageLocation = "./src/app_kvServer/";
	private MetadataHandler metadataHandler;
	protected final int port;
	protected ServerSocket serverSocket = null;
	protected boolean isStopped = true;
	protected CacheManager cacheManager;
	private Logger logger = Logger.getLogger(getClass().getSimpleName());
	private ClientConnection clientConnection;
	private EcsConnection ecsConnection;
	private boolean initialized;
	//  this.serverIp = InetAddress.getLocalHost().getHostAddress();

	public KVServer(int port) {
		this.port = port;
	}

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 * @throws IOException 
	 */

	public void initKVServer(String metadata, int cacheSize, String strategy) {

		DataCache datacache;
		if (strategy.equalsIgnoreCase("FIFO")) {
			datacache = new FIFOStrategy(cacheSize);
		} else if (strategy.equalsIgnoreCase("LRU")) {
			datacache = new LRUStrategy(cacheSize);
		} else {
			datacache = new LFUStrategy(cacheSize);
		}
		try {
			this.cacheManager = new CacheManager(datacache, new Storage(storageLocation));
		} catch (IOException e) {
			logger.fatal("Storage could not be instanciated");
			shutdown();
		}
		metadataHandler = new MetadataHandler("127.0.0.1", port);
		update(metadata);

	}

	public void start() {
		new Thread(this).start();
	}

	private class ECSSocketLoop extends Thread {
		/**
		 * Instanciate socket for future connections
		 * Initiate communication loop with ECS
		 * @return true if succeeded
		 */
		public void run(){
			
				try {
					serverSocket = new ServerSocket(port);
					logger.info("Server listening on port: " 
							+ serverSocket.getLocalPort());
				} catch (IOException e) {
					logger.error("Error! Cannot open server socket:");
					if(e instanceof BindException){
						logger.error("Port " + port + " is already bound!");
					}
				}
				try {
				Socket ecs = serverSocket.accept();                
				ecsConnection = 
						new EcsConnection(port, ecs, KVServer.this);
				new Thread(ecsConnection).start();

				logger.info("Connected to " 
						+ ecs.getInetAddress().getHostName() 
						+  " on port " + ecs.getPort());

			} catch (IOException e) {
				logger.error("Error! " +
						"Unable to establish connection. \n", e);
			}
			logger.info("ECS - Server communication initialized ...");
		}
	}
	/**
	 * Initializes and starts the server. 
	 * Loops until the the server should be closed.
	 */
	@Override
	public void run() {

		isStopped = !initialized;

		if(serverSocket != null) {
			while(!isStopped()){
				try {
					Socket client = serverSocket.accept();                
					clientConnection = 
							new ClientConnection(port, client, cacheManager, metadataHandler);
					new Thread(clientConnection).start();

					logger.info("Connected to " 
							+ client.getInetAddress().getHostName() 
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}


	private synchronized boolean isStopped() {
		return isStopped;
	}

	public synchronized void stop(){
		logger.debug("Stopping the server");
		this.isStopped = true;
		try {
			this.serverSocket.close();
		} catch (IOException e) {
			throw new RuntimeException("Error closing server", e);
		}
	}

	public void shutdown() {
		stop();
		logger.debug("Exiting");
		System.exit(0);
	}

	/**
	 * 
	 * @param rangeToMove contains min and max hash to move
	 * @param destinationServer contains ip and port
	 * @throws IOException
	 */
	public void moveData(String hashOfNewServer, String destinationServerIp, int destinationServerPort) throws IOException {
		cacheManager.flushCache();
		KVStore kvStore = new KVStore(destinationServerIp, destinationServerPort);
		try {
			kvStore.connect();
			for (Pair<String, String> pair : cacheManager) {
				if (metadataHandler.hasToMove(pair.getKey(), hashOfNewServer)) {
					kvStore.put(pair.getKey(), pair.getValue());
					cacheManager.put(pair.getKey(), "null");
				}
			}
		} catch (InterruptedException e) {
			stop();
			logger.error("An error occurred during connection to other server", e);
		} catch (NoSuchAlgorithmException e) {
			logger.fatal("An error occurred during hashing", e);
			shutdown();
		} finally {
			kvStore.disconnect();
		}
	}

	/**
	 * Write the metadata to the known location and update responsability range
	 * @param metadata
	 */
	public void update(String metadata) {
		metadataHandler.update(metadata);
		logger.info("Server metadata updated.");
	}

	public void lockWrite() {
		clientConnection.writeLock();
		logger.info("Server write-locked");
	}

	public void unLockWrite() {
		clientConnection.writeUnlock();
		logger.info("Server write-unlocked");
	}


	/**
	 * Main entry point for the echo server application. 
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			if(args.length != 2) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <loglevel> !");
			} else {
				new LogSetup("logs/server.log", Level.toLevel(args[1]));
				KVServer kvServer = new KVServer(Integer.parseInt(args[0]));
				new Thread(kvServer.new ECSSocketLoop()).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port> <loglevel> !");
			System.exit(1);
		}
	}
}
