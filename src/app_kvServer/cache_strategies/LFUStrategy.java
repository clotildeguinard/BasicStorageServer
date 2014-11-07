package app_kvServer.cache_strategies;

import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import app_kvServer.DataCache;

public class LFUStrategy implements DataCache {
	private final PriorityQueue<Triple<String, String, Integer>> LFUCache;
	private final int capacity;

	public LFUStrategy(int capacity) {
		LFUCache = new PriorityQueue<>(capacity);
		this.capacity = capacity;
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
	 * @return a kvmessage with request status, and the key and value of rejected pair (can be null)
	 */
	@Override
	public KVMessage put(String key, String value) {
		StatusType status = StatusType.PUT_SUCCESS;
		Triple<String, String, Integer> tripleInCache = getTripleIfKeyInCache(key);
		if (tripleInCache != null) {
			LFUCache.remove(tripleInCache);
			status = StatusType.PUT_UPDATE;
		}
		Triple<String, String, Integer> rejected = insert(new Triple<String, String, Integer>(key, value, 0));
		return new KVMessageImpl(rejected.getKey(), rejected.getValue(), status);		
	}
	
	/**
	 * If cache is full, removes kv pair with lowest priority
	 * Inserts new kv pair
	 * @param key
	 * @param value
	 * @return the rejected triple if cache was full
	 */
	private Triple<String, String, Integer> insert(Triple<String, String, Integer> t) {
		Triple<String, String, Integer> rejected = new Triple<String, String, Integer>();
		if (LFUCache.size() == capacity) {
			rejected = LFUCache.poll();
		}
		LFUCache.add(t);
		return rejected;
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
