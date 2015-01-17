package app_kvEcs;

import java.io.IOException;

import app_kvServer.cache_strategies.Strategy;

public interface ConfigCommInterface {
	
	public boolean updateMetadata(String metadata) throws IOException, InterruptedException;
	public boolean lockWrite();
	public boolean unlockWrite();
	public boolean moveData(String[] destinationServer, String minHashToMove,
			String maxHashToMove);
	public boolean copyData(String[] destinationServer, String minHashToMove,
			String maxHashToMove);
	public boolean initKVServer(String metadata, int cacheSize,
			Strategy strategy);
	public boolean startServer();
	public boolean stopServer();
	public boolean shutdown();

}
