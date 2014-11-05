package app_kvServer.cache_strategies;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingDeque;






import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import app_kvServer.DataCache;

public class LRUStrategy implements DataCache {
	private final LinkedList<Pair<String, String>> LRUCache;
	private final int capacity;

	public LRUStrategy(int capacity) {
		LRUCache = new LinkedList<Pair<String, String>>();
		this.capacity = capacity;
	}


	/**
	 * If cache already contains pair with same key, remove it
	 * If cache is full, removes last kv pair
	 * Inserts new kv pair at first position
	 * @return kvmessage with request status, and key and value of rejected pair (can be null)
	 */
	@Override
	public synchronized KVMessage put(String key, String value) {
		StatusType status = StatusType.PUT_SUCCESS;
		String valueInCache = getValueIfKeyInCache(key);
		if (valueInCache != null) {
			status = StatusType.PUT_UPDATE;
			LRUCache.remove(new Pair<String, String>(key, valueInCache));
		}
		Pair<String, String> rejected = insert(key, value);
		return new KVMessageImpl(rejected.getKey(), rejected.getValue(), status);
	}

	/**
	 * If cache is full, removes last kv pair
	 * Inserts new kv pair at first position
	 * @param key
	 * @param value
	 * @return rejected pair if cache was full
	 */
	private Pair<String, String> insert(String key, String value) {
		Pair<String, String> rejected = new Pair<String, String>();
		if (LRUCache.size() == capacity) {
			rejected = LRUCache.removeLast();
		}
		LRUCache.addFirst(new Pair<String, String>(key, value));
		return rejected;
	}

	/**
	 * If pair is in cache, re-insert it at first position
	 * And return pair
	 * Else return null
	 */
	@Override
	public synchronized KVMessage get(String key) {
		String value = getValueIfKeyInCache(key);
		if (value == null) {
			return null;
		} else {
			LRUCache.remove(new Pair<String, String>(key, value));
			put(key, value);
			return new KVMessageImpl(key, value, StatusType.GET_SUCCESS);
		}
	}

	private String getValueIfKeyInCache(String key) {
		for (Pair<String, String> pair : LRUCache) {
			if (pair.getKey().equals(key)) {
				return pair.getValue();
			}
		}
		return null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Pair<String, String> pair : LRUCache) {
			sb.append(pair.getKey() + " - " + pair.getValue() + " / ");
		}
		return sb.toString();
	}




}
