package common.metadata;


public class Address {
	private final static String equivLocalhost = "127.0.0.1";
	private String ip;
	private int port;

	public Address(String ip, int port) {
		if (ip != null && ip.equals(equivLocalhost)) {
			this.ip = "localhost";
		} else {
			this.ip = ip;
		}
		this.port = port;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	public boolean isSameAddress(Address a) {
		return ((ip == null && a.getIp() == null) || ip.equals(a.getIp()))
				&& (port == a.getPort());
	}

	@Override
	public String toString() {
		return ip + ":" + port;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof Address)) {
			return false;
		}
		return this.isSameAddress((Address) o);
	}

	@Override
	public int hashCode() {
		return (this.toString()).hashCode();
	}


}
