package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import logger.ServerLogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.cache_strategies.FIFOStrategy;
import app_kvServer.cache_strategies.LFUStrategy;
import app_kvServer.cache_strategies.LRUStrategy;


public class KVServer implements Runnable {
	private final String storageLocation = "./src/app_kvServer/";
	protected int Port = 50000;
	protected ServerSocket serverSocket = null;
	protected boolean isStopped = true;
	protected Thread runningThread = null;
	protected final CacheManager cacheManager;
	private Logger logger = Logger.getRootLogger();
	
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

	public KVServer(int port, int cacheSize, String strategy) throws IOException {
		this.Port = port;
		DataCache datacache;
		if (strategy.equalsIgnoreCase("FIFO")) {
			datacache = new FIFOStrategy(cacheSize);
		} else if (strategy.equalsIgnoreCase("LRU")) {
			datacache = new LRUStrategy(cacheSize);
		} else {
			datacache = new LFUStrategy(cacheSize);
		}
		this.cacheManager = new CacheManager(datacache, new Storage(storageLocation));
	}
	  /**
     * Initializes and starts the server. 
     * Loops until the the server should be closed.
     */
	@Override
    public void run() {
        
    	isStopped = !initializeServer();
        
        if(serverSocket != null) {
	        while(!isStopped()){
	            try {
	                Socket client = serverSocket.accept();                
	                ServerRunnable connection = 
	                		new ServerRunnable(client, "Multithreaded KVServer", cacheManager);
	                new Thread(connection).start();
	                
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
        this.isStopped = true;
        try {
            this.serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing server", e);
        }
    }

    private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(Port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + Port + " is already bound!");
            }
            return false;
        }
    }
	
	/**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
    		new ServerLogSetup("logs/server.log", Level.ALL);
			if(args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cacheSize> <cacheStrategy>!");
				System.out.println("\t <cacheStrategy>: FIFO | LRU | LFU");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				
				if (cacheSize < 0) {
					System.out.println("Error! Invalid argument <cacheSize>! Must be positive or zero!");
					System.exit(1);
				}
				new Thread(new KVServer(port, cacheSize, strategy)).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port> or <cacheSize>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
    }
}
