package app_kvServer;


import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.metadata.Address;
import common.metadata.MetadataHandlerServer;
import app_kvApi.KVStore;
import app_kvApi.KVStoreServer;
import app_kvServer.cache_strategies.FIFOStrategy;
import app_kvServer.cache_strategies.LFUStrategy;
import app_kvServer.cache_strategies.LRUStrategy;
import app_kvServer.cache_strategies.Pair;

public class KVServer {
	private static final String storageLocation = "./src/app_kvServer/";
	private static final Logger logger = Logger.getLogger(KVServer.class);
	protected final int port;

	private MetadataHandlerServer metadataHandler;
	protected ServerSocket serverSocket = null;
	protected CacheManager cacheManager;
	private EcsConnection ecsConnection;
	private HeartBeatHandler heartBeatHandler;

	protected boolean isStopped = true;
	private boolean shutdown = false;
	private boolean writeLocked = false;
	private boolean initialized = false;

	/**
	 * Launch KVServer at given port,
	 * wait for ECS to connect to it
	 * @param port
	 */
	public KVServer(int port) {
		this.port = port;
		restartEcsConnection();
	}

	public void restartEcsConnection() {
		Thread ecsWaitingThread = new Thread(new ECSWaitingThread());
		ecsWaitingThread.start();
	}

	/**
	 * Initialize KVServer,
	 * begin to listen to clients
	 * and launch heartbeat
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
			logger.fatal("Storage could not be instanciated");
			shutdown();
		}
		metadataHandler = new MetadataHandlerServer("127.0.0.1", port);
		updateMetadata(metadata);

		initialized = true;

		new Thread(new ClientAccepterThread()).start();
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
		stop();
		stopHeartbeat();
		try {
			if (serverSocket != null) {
				serverSocket.close();
			}
		} catch (IOException e) {
			// Do nothing.
		}
		shutdown = true;
	}

	private void openServerSocket() throws IOException {
		try {
			serverSocket = new ServerSocket();
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(port));
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

	public class ECSWaitingThread implements Runnable {
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
					logger.fatal("Server socket could not be opened; kvServer cannot run.");
					return;
				}
			}
			try {
				Socket ecs = serverSocket.accept();   
				ecsConnection = 
						new EcsConnection(port, ecs, KVServer.this);
				ecsConnection.start();

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
	 * Loops until the server should be closed.
	 */
	public class ClientAccepterThread implements Runnable {
		@Override
		public void run() {
			if (initialized) {
				logger.debug("Server listening to clients...");
			} else {
				logger.error("Server not initialized");
				return;
			}

			if (serverSocket == null) {
				try {
					openServerSocket();
				} catch (IOException e) {
					logger.fatal("Server socket could not be opened; kvServer cannot run.");
					return;
				}
			}

			while (!shutdown){
				try {
					Socket client = serverSocket.accept();
					logger.debug("Connected to client " 
							+ client.getInetAddress().getHostName() 
							+  " on port " + client.getPort());

					ClientConnection clientConnection = 
							new ClientConnection(port, client, cacheManager, metadataHandler, writeLocked, isStopped, heartBeatHandler);
					new Thread(clientConnection).start();
				} catch (IOException e) {
					if (!shutdown) {
						logger.error("Unable to establish connection with a client. \n", e);
						shutdown();
					}
				}
			}
			try {
				serverSocket.close();
			} catch (IOException e) {
				logger.error("Error closing connection with clients. \n", e);
			}
			logger.info("Server stopped to listen to clients.");
		}
	}


	/**
	 * Copy to given node all data having key-hash between minHash and maxHash
	 * @param minHashValue
	 * @param maxHashValue
	 * @param destinationServerIp
	 * @param destinationServerPort
	 * @throws IOException
	 */
	public void copyData(String minHashValue, String maxHashValue, Address destinationServer)
			throws IOException {
		cacheManager.flushCache();
		logger.debug("Cache flushed.");
		KVStore kvStore = new KVStoreServer(destinationServer);
		try {
			kvStore.connect();
			for (Pair<String, String> pair : cacheManager) {
				if (metadataHandler.isInRange(pair.getKey(), minHashValue, maxHashValue)) {
					logger.debug("Move key " + pair.getKey() + ", value " + pair.getValue());
					kvStore.put(pair.getKey(), pair.getValue());
					cacheManager.put(pair.getKey(), "null");
				}
				else {
					logger.debug("Keep key " + pair.getKey() + ", value " + pair.getValue());
				}
			}
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
	 * Erase from storage all data having key-hash between minHash and maxHash
	 * @param minHash
	 * @param maxHash
	 * @throws IOException
	 */
	public void deleteData(String minHash, String maxHash) throws IOException {
		cacheManager.flushCache();
		logger.debug("Cache flushed.");
		try {
			for (Pair<String, String> pair : cacheManager) {
				if (metadataHandler.isInRange(pair.getKey(), minHash, maxHash)) {
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

	public void startHeartbeat() {
		if (heartBeatHandler == null) {
			heartBeatHandler = new HeartBeatHandler(metadataHandler, ecsConnection);
			new Thread(heartBeatHandler).start();
		}
	}

	public void stopHeartbeat() {
		if (heartBeatHandler != null) {
			heartBeatHandler.shutdown();
			try {
				heartBeatHandler.join();
				heartBeatHandler = null;
			} catch (InterruptedException e) {
				logger.error("Interrupted when trying to join heartbeat thread");
			}
		}

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
				new KVServer(Integer.parseInt(args[0]));
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
