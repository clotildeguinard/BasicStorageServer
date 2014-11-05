package app_kvServer.cache_strategies;

import java.util.concurrent.LinkedBlockingDeque;





import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import app_kvServer.DataCache;

public class LRUStrategy implements DataCache {
	private final LinkedBlockingDeque<Pair<String, String>> LRUCache;

	public LRUStrategy(int capacity) {
		LRUCache = new LinkedBlockingDeque<Pair<String, String>>(capacity);
	}

	public static void main(String[] args) {
		DataCache cache = new LRUStrategy(3);
		cache.put("foo", "bar"); System.out.println(cache.toString());
		cache.put("hello", "zebra"); System.out.println(cache.toString());
		cache.put("cat", "dog"); System.out.println(cache.toString());
		cache.put("foo", "bar"); System.out.println(cache.toString());
		
		cache.get("cat"); System.out.println(cache.toString());
		
		cache.put("cat", "mouse"); System.out.println(cache.toString());
		cache.put("cat", "mouse"); System.out.println(cache.toString());
		cache.put("hi", "new"); System.out.println(cache.toString());

	}

	/**
	 * If cache already contains pair with same key, remove it
	 * If cache is full, removes last kv pair
	 * Inserts new kv pair at first position
	 */
	@Override
	public KVMessage put(String key, String value) {
		StatusType status = StatusType.PUT_SUCCESS;
		String valueInCache = getValueIfKeyInCache(key);

		if (valueInCache != null) {
			LRUCache.remove(new Pair<String, String>(key, valueInCache));
			status = StatusType.PUT_UPDATE;
		}
		insert(key, value);
		return new KVMessageImpl(key, value, status);
	}

	/**
	 * If cache is full, removes last kv pair
	 * Inserts new kv pair at first position
	 * @param key
	 * @param value
	 */
	private void insert(String key, String value) {
		if (!LRUCache.offerFirst(new Pair<String, String>(key, value))) {
			LRUCache.removeLast();
			put(key, value);
		}
	}

	/**
	 * If pair is in cache, re-insert it at first position
	 * And return pair
	 * Else return null
	 */
	@Override
	public KVMessage get(String key) {
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
			sb.append(pair.getKey() + " - " + pair.getValue() + " ///// ");
		}
		return sb.toString();
	}




}
