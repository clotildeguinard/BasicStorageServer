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

	@Override
	public synchronized KVMessage get(String key) {
		Pair<String, String> p = getPairIfKeyInCache(key);
		if (p != null) {
			return new KVMessageImpl(key, p.getValue(), StatusType.GET_SUCCESS);

		}
		return null;
	}

	@Override
	public synchronized KVMessage put(String key, String value) {
		Pair<String, String> p = getPairIfKeyInCache(key);
		FIFOCache.remove(p);
		if (FIFOCache.size() == capacity) {
			FIFOCache.removeLast();
		}
		FIFOCache.addFirst(new Pair<String, String>(key, value));
		return null;
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


