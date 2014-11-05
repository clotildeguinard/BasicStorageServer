package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class CacheManager {
	private DataCache dataCache;
	private Storage storage;
	
	public CacheManager(DataCache dataCache, Storage storage) {
		this.dataCache = dataCache;
		this.storage = storage;
	}

	public KVMessage put(String key, String value)  {

		KVMessage storageAnswer = storage.put(key, value);;

		if (storageAnswer.getStatus() != StatusType.PUT_ERROR) {
			dataCache.put(key, value);
		}
		return storageAnswer;
	}


	public KVMessage get(String key) {
		KVMessage cacheAnswer = dataCache.get(key);
		if (cacheAnswer != null) {
			return cacheAnswer;
		} else {

			KVMessage storageAnswer = storage.get(key);

			dataCache.put(storageAnswer.getKey(), storageAnswer.getValue());
			return storageAnswer;
		}
	}

}
