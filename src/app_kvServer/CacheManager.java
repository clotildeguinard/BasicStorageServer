package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class CacheManager {
	private DataCache dataCache;
	
	public CacheManager(DataCache dataCache) {
		this.dataCache = dataCache;
	}

	public KVMessage put(String key, String value)  {
		//TODO
		// write to file (must return a kvmessage)
		KVMessage putAnswer = null;

		if (putAnswer.getStatus() != StatusType.PUT_ERROR) {
			dataCache.put(key, value);
		}
		return putAnswer;
	}


	public KVMessage get(String key) {
		KVMessage cacheAnswer = dataCache.get(key);
		if (cacheAnswer != null) {
			return cacheAnswer;
		} else {
			//TODO

			//read in file (must return a kvmessage)
			KVMessage storageAnswer = null;

			dataCache.put(storageAnswer.getKey(), storageAnswer.getValue());
			return storageAnswer;
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
