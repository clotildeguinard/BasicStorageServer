package app_kvServer;

import common.messages.KVMessage;


public interface DataCache {
		

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
}
