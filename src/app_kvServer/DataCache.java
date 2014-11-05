package app_kvServer;

import common.messages.KVMessage;


public interface DataCache {
		
	public KVMessage put(String Key, String Value);
	
	public KVMessage get(String Key);
}
