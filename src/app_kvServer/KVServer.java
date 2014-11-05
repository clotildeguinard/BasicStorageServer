package app_kvServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import app_kvServer.cache_strategies.FIFO;
import app_kvServer.cache_strategies.LFUStrategy;
import app_kvServer.cache_strategies.LRUStrategy;


public class KVServer implements Runnable {
	
	protected int Port = 50000;
	protected ServerSocket serverSocket = null;
	protected boolean isStopped = false;
	protected Thread runningThread = null;
	protected CacheManager cacheManager;
	
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 */

	public KVServer(int port, int cacheSize, String strategy) {
		this.Port = port;
		DataCache datacache;
		if (strategy.equalsIgnoreCase("FIFO")) {
			datacache = new FIFO(cacheSize);
		} else if (strategy.equalsIgnoreCase("LRU")) {
			datacache = new LRUStrategy(cacheSize);
		} else {
			datacache = new LFUStrategy(cacheSize);
		}
		this.cacheManager = new CacheManager(datacache);
	}

	@Override
	public void run() {
		synchronized(this){
			this.runningThread = Thread.currentThread();
		}
		openServerSocket();
		
		while(!isStopped()){
			Socket clientSocket = null;
			try{
				clientSocket = this.serverSocket.accept();
			} catch (IOException e){
				if(isStopped()){
					System.out.println("Server Stopped.") ;
                    return;
                }
                throw new RuntimeException(
                    "Error accepting client connection", e);
            }
			new Thread(new WorkerRunnable(clientSocket, "Multithreaded Server")).start();
		}		
	}
	
	private synchronized boolean isStopped() {
		return this.isStopped;
	}
	
    public synchronized void stop(){
        this.isStopped = true;
        try {
            this.serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing server", e);
        }
    }

	private void openServerSocket() {
		try {
            this.serverSocket = new ServerSocket(this.Port);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port 5000", e);
        }
	}
	
	public void main(String args[]){
		KVServer server = new KVServer(9000, 20, "FIFO");
		new Thread(server).start();

		try {
		    Thread.sleep(20 * 1000);
		} catch (InterruptedException e) {
		    e.printStackTrace();
		}
		System.out.println("Stopping Server");
		server.stop();
		
	}
}
