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
	private boolean writeLocked = false;

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
			this.cacheManager = new CacheManager(datacache, new Storage(storageLocation, Integer.toString(port)));
		} catch (IOException e) {
			logger.fatal("Storage could not be instanciated");
			shutdown();
		}
		metadataHandler = new MetadataHandler("127.0.0.1", port);
		updateMetadata(metadata);
		if (serverSocket == null) {
			openServerSocket();
		}

		initialized = true;
	}

	public void start() {
		new Thread(this).start();
	}

	private void openServerSocket() {
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
	}

	private class ECSSocketLoop extends Thread {
		/**
		 * Instanciate socket for future connections
		 * Initiate communication loop with ECS
		 * @return true if succeeded
		 */
		public void run(){
			if (serverSocket == null) {
				openServerSocket();
			}
			try {
				Socket ecs = serverSocket.accept();                
				ecsConnection = 
						new EcsConnection(port, ecs, KVServer.this);
				new Thread(ecsConnection).start();

				logger.info("Connected to ECS " 
						+ ecs.getInetAddress().getHostName() 
						+  " on port " + ecs.getPort());

			} catch (IOException e) {
				logger.error("Error! " +
						"Unable to establish connection. \n", e);
			}
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
			logger.debug("Server listening to clients...");
			while(!isStopped()){
				try {
					Socket client = serverSocket.accept();                
					clientConnection = 
							new ClientConnection(port, client, cacheManager, metadataHandler, writeLocked);
					new Thread(clientConnection).start();

					logger.info("Connected to " 
							+ client.getInetAddress().getHostName() 
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
			try {
				serverSocket.close();
			} catch (IOException e) {
				throw new RuntimeException("Error closing connection with clients.", e);
			}
		}
		logger.info("Server stopped.");
	}


	private boolean isStopped() {
		return isStopped;
	}

	public void stop(){
		logger.debug("Stopping the server");
		this.isStopped = true;
	}

	public void shutdown() {
		stop();
		logger.info("Exiting");
		System.exit(0);
	}

	/**
	 * Move to given node all data having key under hash hashOfNewNode
	 * @param hashOfNewNode the hash of the new node
	 * @param destinationServerIp
	 * @param destinationServerPort
	 * @throws IOException
	 */
	public void moveData(String hashOfNewNode, String destinationServerIp, int destinationServerPort)
			throws IOException {
		cacheManager.flushCache();
		logger.debug("Cache flushed.");
		KVStore kvStore = new KVStore(destinationServerIp, destinationServerPort);
		try {
			kvStore.connect();
			for (Pair<String, String> pair : cacheManager) {
				if (metadataHandler.hasToMove(pair.getKey(), hashOfNewNode)) {
					logger.debug("Move key " + pair.getKey() + ", value " + pair.getValue());
					kvStore.put(pair.getKey(), pair.getValue());
					cacheManager.put(pair.getKey(), "null");
				}
				else {
					logger.debug("Keep key " + pair.getKey() + ", value " + pair.getValue());
				}
			}
			eraseMovedData(hashOfNewNode);
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
	 * Erase from storage all data having key under hash hashOfNewNode
	 * @param hashOfNewNode the hash of the new node
	 * @throws IOException
	 */
	public void eraseMovedData(String hashOfNewNode) throws IOException {
		cacheManager.flushCache();
		logger.debug("Cache flushed.");
		try {
			for (Pair<String, String> pair : cacheManager) {
				if (metadataHandler.hasToMove(pair.getKey(), hashOfNewNode)) {
					logger.debug("Erase key " + pair.getKey() + ", value " + pair.getValue());
					cacheManager.put(pair.getKey(), "null");
				}
				else {
					logger.debug("Keep key " + pair.getKey() + ", value " + pair.getValue());
				}
			}
		} catch (NoSuchAlgorithmException e) {
			logger.fatal("An error occurred during hashing", e);
			shutdown();
		}
	}

	/**
	 * Write the metadata to the known location and update responsability range
	 * @param metadata
	 */
	public void updateMetadata(String metadata) {
		metadataHandler.update(metadata);
		logger.info("Server metadata updated.");
	}

	public void lockWrite() {
		writeLocked = true;
		if (clientConnection != null) {
			clientConnection.writeLock();
		}
		logger.info("Server write-locked");
	}

	public void unLockWrite() {
		writeLocked = false;
		if (clientConnection != null) {
			clientConnection.writeUnlock();
		}
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
