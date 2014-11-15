package app_kvServer;


import app_kvServer.cache_strategies.Pair;
import common.messages.KVMessage;


public interface DataCache extends Iterable<Pair<String, String>> {
		

	/**
	 * Write pair into cache
	 * @param Key
	 * @param Value
	 * @return A kvmessage containing the pair that was rejected from the cache, null if none was rejected
	 */
	public KVMessage put(String Key, String Value);
	
	/**
	 * 
	 * @param Key
	 * @return A kvmessage containing the relevant pair, null if key not in cache
	 */
	public KVMessage get(String Key);

	public void erase();
}
