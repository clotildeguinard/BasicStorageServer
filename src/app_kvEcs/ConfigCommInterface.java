package app_kvEcs;

import java.io.IOException;

import app_kvServer.cache_strategies.Strategy;
import common.messages.KVAdminMessage;

public interface ConfigCommInterface {
	
	public KVAdminMessage updateMetadata(String metadata) throws IOException, InterruptedException;
	public KVAdminMessage lockWrite();
	public KVAdminMessage unlockWrite();
	public KVAdminMessage startServer();
	public KVAdminMessage stopServer();
	public KVAdminMessage shutdown();
	KVAdminMessage moveData(String hashOfNewServer, String[] destinationServer);
	KVAdminMessage initKVServer(String metadata, int cacheSize,
			Strategy strategy);

}
