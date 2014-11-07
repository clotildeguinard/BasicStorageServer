package app_kvServer.cache_strategies;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingDeque;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import app_kvServer.DataCache;

public class FIFOStrategy implements DataCache{
	private LinkedList<Pair<String, String>> FIFOCache;
	private int capacity;

	public FIFOStrategy(int capacity) {
		FIFOCache = new LinkedList<>();
		this.capacity = capacity;
	}
	
	/**
	 * @return kvmessage if key is in cache, null otherwise
	 */
	@Override
	public KVMessage get(String key) {
		Pair<String, String> p = getPairIfKeyInCache(key);
		if (p != null) {
			return new KVMessageImpl(key, p.getValue(), StatusType.GET_SUCCESS);

		}
		return null;
	}

	/**
	 * If cache already contains pair with same key, remove it
	 * Inserts new kv pair at first position
	 * @return a kvmessage with request status, and the key and value of rejected pair (can be null)
	 */
	@Override
	public KVMessage put(String key, String value) {
		StatusType status = StatusType.PUT_SUCCESS;
		Pair<String, String> p = getPairIfKeyInCache(key);
		if (p != null) {
			FIFOCache.remove(p);
			status = StatusType.PUT_UPDATE;
		}
		Pair<String, String> rejected = new Pair<>();
		if (FIFOCache.size() == capacity) {
			rejected = FIFOCache.removeLast();
		}
		FIFOCache.addFirst(new Pair<String, String>(key, value));
		return new KVMessageImpl(rejected.getKey(), rejected.getValue(), status);
	}

	private Pair<String, String> getPairIfKeyInCache(String key) {
		for (Pair<String, String> pair : FIFOCache) {
			if (pair.getKey().equals(key)) {
				return pair;
			}
		}
		return null;
	}
	

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Pair<String, String> pair : FIFOCache) {
			sb.append(pair.getKey() + " - " + pair.getValue() + " / ");
		}
		return sb.toString();
	}
}


