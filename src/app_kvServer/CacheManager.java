package app_kvServer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.Logger;

import app_kvServer.cache_strategies.FIFOStrategy;
import app_kvServer.cache_strategies.Pair;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;

public class CacheManager implements Iterable<Pair<String, String>> {
	private DataCache dataCache;
	private Storage storage;
	private Logger logger = Logger.getRootLogger();

	
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
	public synchronized KVMessage put(String key, String value)  {

		KVMessage cacheAnswerWithRejectedKV = dataCache.put(key, value);
		if (cacheAnswerWithRejectedKV.getKey() != null) {
			try {
				storage.put(cacheAnswerWithRejectedKV.getKey(), cacheAnswerWithRejectedKV.getValue());
			} catch (FileNotFoundException fnfe) {
				logger.fatal("The storage file could not be found", fnfe);
			} catch (IOException e) {
				logger.error("A connection error occurred when trying to write some record", e);
				return new KVMessageImpl(key, value, StatusType.PUT_ERROR);
			}
		}
		StatusType status = cacheAnswerWithRejectedKV.getStatus();

		if (status.equals(StatusType.PUT_SUCCESS)) {
			try {
				KVMessage checkIfOverwrite = storage.get(key);
				if (checkIfOverwrite.getStatus().equals(StatusType.GET_SUCCESS)) {
					status = StatusType.PUT_UPDATE;
				}
			} catch (FileNotFoundException e) {
				logger.error("A file was not found when trying to write the record", e);
				return new KVMessageImpl(key, value, StatusType.PUT_ERROR);
			} catch (IOException e) {
				logger.error("A connection error occurred when trying to write the record", e);
				return new KVMessageImpl(key, value, StatusType.PUT_ERROR);
			}
		}
		
		if (value.equals("null")) {
			if (status.equals(StatusType.PUT_SUCCESS) || status.equals(StatusType.PUT_UPDATE)) {		
				return new KVMessageImpl(key, value, StatusType.DELETE_SUCCESS);
			} else if (status.equals(StatusType.PUT_ERROR)) {
				return new KVMessageImpl(key, value, StatusType.DELETE_ERROR);
			}
		}
		
		return new KVMessageImpl(key, value, status);
	}


	public synchronized KVMessage get(String key) {
		KVMessage cacheAnswer = dataCache.get(key);
		if (cacheAnswer != null) {
			return cacheAnswer;
		} else {

			KVMessage storageAnswer;
			try {
				storageAnswer = storage.get(key);
				if (storageAnswer.getValue() != null) {
					dataCache.put(storageAnswer.getKey(), storageAnswer.getValue());
					return new KVMessageImpl(storageAnswer.getKey(), storageAnswer.getValue(), StatusType.GET_SUCCESS);
				}
				return storageAnswer;
				
			} catch (FileNotFoundException e) {
				logger.error("A file was not found when trying to write the record", e);
				return new KVMessageImpl(key, null, StatusType.GET_ERROR);
			} catch (IOException e) {
				logger.error("A connection error occurred when trying to write the record", e);
				return new KVMessageImpl(key, null, StatusType.GET_ERROR);
			}
		}
	}
    
    public void flushCache() throws IOException {
    	for (Pair<String, String> kv : dataCache) {
    		storage.put(kv.getKey(), kv.getValue());
    	}
    	dataCache.erase();
    }

	@Override
	public Iterator<Pair<String, String>> iterator() {
		return storage.iterator();
	}

}
