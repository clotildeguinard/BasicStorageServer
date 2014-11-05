package app_kvServer.cache_strategies;

public class Pair<L,R> {
    final L key;
	final R value;
	
	public Pair() {
		this.key = null;
		this.value = null;
	}

    public Pair(L key, R value) {
      this.key = key;
      this.value = value;
    }

    public L getKey() {
		return key;
	}

	public R getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		Pair<String, String> p = (Pair<String, String>) o;
		return this.getKey().equals(p.getKey()) && this.getValue().equals(p.getValue());
	}
}