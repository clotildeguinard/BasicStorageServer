package app_kvServer.cache_strategies;

import java.util.concurrent.PriorityBlockingQueue;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import app_kvServer.DataCache;

public class LFUStrategy implements DataCache {
	private final PriorityBlockingQueue<Triple<String, String, Integer>> LFUCache;
	private final int capacity;

	public LFUStrategy(int capacity) {
		LFUCache = new PriorityBlockingQueue<>(capacity);
		this.capacity = capacity;
	}

	public static void main(String[] args) {
		DataCache cache = new LFUStrategy(3);
		cache.put("foo", "bar"); System.out.println(cache.toString());
		cache.put("hello", "zebra"); System.out.println(cache.toString());
		cache.put("cat", "dog"); System.out.println(cache.toString());
		
		cache.get("cat"); System.out.println(cache.toString());
		cache.get("cat"); System.out.println(cache.toString());
		cache.get("cat"); System.out.println(cache.toString());
		cache.get("hello"); System.out.println(cache.toString());
		
		cache.put("cat", "mouse"); System.out.println(cache.toString());
		cache.put("hi", "new"); System.out.println(cache.toString());

	}


	/**
	 * If pair is in cache, re-insert with higher priority
	 * And return pair
	 * Else return null
	 */
	@Override
	public KVMessage get(String key) {
		Triple<String, String, Integer> t = getTripleIfKeyInCache(key);
		if (t == null) {
			return null;
		} else {
			incrementPriority(t);
			return new KVMessageImpl(key, t.getValue(), StatusType.GET_SUCCESS);
		}
	}

	private void incrementPriority(Triple<String, String, Integer> t) {
		LFUCache.remove(t);
		insert(new Triple<String, String, Integer>(t.getKey(), t.getValue(), t.getPriority() + 1));	
	}

	/**
	 * If cache already contains pair with same key, remove it
	 * Inserts new kv pair with priority 0
	 * @return 
	 */
	@Override
	public KVMessage put(String key, String value) {
		StatusType status = StatusType.PUT_SUCCESS;
		Triple<String, String, Integer> tripleInCache = getTripleIfKeyInCache(key);
		if (tripleInCache != null) {
			LFUCache.remove(tripleInCache);
			status = StatusType.PUT_UPDATE;
		}
		insert(new Triple<String, String, Integer>(key, value, 0));
		return new KVMessageImpl(key, value, status);		
	}
	
	/**
	 * If cache is full, removes kv pair with lowest priority
	 * Inserts new kv pair
	 * @param key
	 * @param value
	 */
	private void insert(Triple<String, String, Integer> t) {
		if (LFUCache.size() == capacity) {
			LFUCache.poll();
		}
		LFUCache.add(t);
	}

	private Triple<String, String, Integer> getTripleIfKeyInCache(String key) {
		for (Triple<String, String, Integer> triple : LFUCache) {
			if (triple.getKey().equals(key)) {
				return triple;
			}
		}
		return null;
	}


	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Triple<String, String, Integer> pair : LFUCache) {
			sb.append(pair.getKey() + " - " + pair.getValue() + " - " + pair.getPriority() + " / ");
		}
		return sb.toString();
	}

}
