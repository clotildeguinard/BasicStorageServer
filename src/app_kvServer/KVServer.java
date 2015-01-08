package app_kvServer;


import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.communication.HeartBeat;
import common.messages.KVAdminMessage;
import common.messages.KVMessage;
import common.metadata.MetadataHandler;
import app_kvClient.KVStore;
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
	private EcsConnection ecsConnection;
	private List<KVAdminMessage> suspiciousQueue;
	private boolean shutdown = false;
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

	public void initKVServer(String metadata, int cacheSize, String strategy) throws IOException {

		DataCache datacache;
		if (strategy.equalsIgnoreCase("FIFO")) {
			datacache = new FIFOStrategy(cacheSize);
		} else if (strategy.equalsIgnoreCase("LRU")) {
			datacache = new LRUStrategy(cacheSize);
		} else {
			datacache = new LFUStrategy(cacheSize);
		}
		try {
			this.cacheManager = new CacheManager(datacache,
					new Storage(storageLocation, Integer.toString(port)));
		} catch (IOException e) {
			stop();
			logger.fatal("Storage could not be instanciated");
			throw(e);
		}
		metadataHandler = new MetadataHandler("127.0.0.1", port);
		updateMetadata(metadata);
		if (serverSocket == null) {
			openServerSocket();
		}

		shutdown = false;
		new Thread(this).start();
	}

	public void start() {
		logger.debug("Starting the server");
		isStopped = false;
	}

	public void stop(){
		logger.debug("Stopping the server");
		this.isStopped = true;
	}

	public void shutdown() {
		shutdown = true;
	}

	private void openServerSocket() throws IOException {
		try {
			serverSocket = new ServerSocket();
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(port));

			//			serverSocket = new ServerSocket(port);

			logger.info("Server listening on port: " 
					+ serverSocket.getLocalPort());
		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			throw(e);
		}
	}

	public class ECSSocketLoop extends Thread {
		/**
		 * Instanciate socket for future connections
		 * Initiate communication loop with ECS
		 * @return true if succeeded
		 */
		public void run(){
			if (serverSocket == null) {
				try {
					openServerSocket();
				} catch (IOException e) {
					logger.error("Server socket could not be opened; kvServer cannot run.");
					return;
				}
			}
			try {
				Socket ecs = serverSocket.accept();                
				ecsConnection = 
						new EcsConnection(port, ecs, KVServer.this, suspiciousQueue);
				new Thread(ecsConnection).start();

				logger.info("Connected to ECS " 
						+ ecs.getInetAddress().getHostName() 
						+  " on port " + ecs.getPort());

			} catch (IOException e) {
				logger.error("Error! " +
						"Unable to establish connection with ECS. \n", e);
			}
		}
	}
	/**
	 * Initializes and starts the server. 
	 * Loops until the the server should be closed.
	 */
	@Override
	public void run() {

		if (serverSocket == null) {
			return;
		}
		logger.debug("Server listening to clients...");
		List<KVMessage> heartbeatQueue = new ArrayList<KVMessage>();
		new Thread(new HeartBeat(metadataHandler, heartbeatQueue, isStopped, suspiciousQueue)).start();
		while (!shutdown){
			try {
				Socket client = serverSocket.accept();
				logger.info("Connected to client " 
						+ client.getInetAddress().getHostName() 
						+  " on port " + client.getPort());
				
				ClientConnection clientConnection = 
						new ClientConnection(port, client, cacheManager, metadataHandler, writeLocked, isStopped, heartbeatQueue);
				new Thread(clientConnection).start();
			} catch (SocketException e1) {
				shutdown = true;
				logger.error("Unable to establish connection with a client. \n", e1);
				logger.debug("Server socket bound : " + serverSocket.isBound());
				logger.debug("Server socket closed : " + serverSocket.isClosed());
			} catch (IOException e) {
				shutdown = true;
				logger.error("Unable to establish connection with a client. \n", e);
			}
		}
		logger.info("Stopping to listen to clients");
		try {
			serverSocket.close();
		} catch (IOException e) {
			throw new RuntimeException("Error closing connection with clients.", e);
		}
		logger.info("Server stopped.");
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
			stop();
			logger.fatal("An error occurred during hashing", e);
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
			stop();
			System.exit(1);
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
		logger.info("Server write-locked");
	}

	public void unLockWrite() {
		writeLocked = false;
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
