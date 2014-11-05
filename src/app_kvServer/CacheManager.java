package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;

public class CacheManager {
	private DataCache dataCache;
	private Storage storage;
	
	public CacheManager(DataCache dataCache, Storage storage) {
		this.dataCache = dataCache;
		this.storage = storage;
	}

	/**
	 * write the kv to the cache
	 * write the rejected kv to the storage if not null
	 * if cache returns SUCCESS, check in storage whether it is a SUCCESS or an UPDATE
	 * @param key
	 * @param value
	 * @return a kvmessage with status of the request
	 */
	public KVMessage put(String key, String value)  {

		KVMessage cacheAnswerWithRejectedKV = dataCache.put(key, value);
		if (cacheAnswerWithRejectedKV.getKey() != null) {
			storage.put(cacheAnswerWithRejectedKV.getKey(), cacheAnswerWithRejectedKV.getValue());
		}
		StatusType status = cacheAnswerWithRejectedKV.getStatus();

		if (status.equals(StatusType.PUT_SUCCESS)) {

			KVMessage checkIfOverwrite = storage.get(key);
			if (checkIfOverwrite.getStatus().equals(StatusType.GET_SUCCESS)) {
				status = StatusType.PUT_UPDATE;
			}
		}

		return new KVMessageImpl(key, value, status);
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
