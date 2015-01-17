package common.metadata;


public class Address {
	private final static String equivLocalhost = "127.0.0.1";
	private String ip;
	private int port;
	
	public Address(String ip, int port) {
		this.ip = ip;
		this.port = port;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}
	
	public boolean isSameAddress(Address a) {
		return this.equalsIp(a.getIp())
				&& (getPort() == a.getPort());
	}

	private boolean equalsIp(String ip1) {
		return (ip.equals(ip1)
				|| (ip.equals("localhost") && ip1.equals(equivLocalhost))
				|| (ip.equals(equivLocalhost) && ip1.equals("localhost")));
	}
	
	public String toString() {
		return ip + ":" + port;
	}
	
	
	
	

}
