package app_kvServer.cache_strategies;

public class Triple<L,R, Integer> implements Comparable<Triple<L,R, Integer>> {
    private final L key;
	private final R value;
	private int priority;

    public Triple(L key, R value, int priority) {
      this.key = key;
      this.value = value;
      this.priority = priority;
    }

    public L getKey() {
		return key;
	}

	public R getValue() {
		return value;
	}

	public int getPriority() {
		return priority;
	}

	public Triple incrementPriority() {
		priority ++;
		return this;
	}

	@Override
	public boolean equals(Object o) {
		Triple<String, String, Integer> t = (Triple<String, String, Integer>) o;
		return this.getKey().equals(t.getKey()) && this.getValue().equals(t.getValue());
	}

	@Override
	public int compareTo(Triple<L, R, Integer> t) {
		return this.getPriority() - t.getPriority();
	}
}