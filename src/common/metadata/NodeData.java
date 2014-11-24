package common.metadata;

public class NodeData {
	private String ipAddress;
	private int portNumber;
	private String minHashKey;
	private String maxHashKey;
	private String name;
	
	public NodeData(String ipAddress, int portNumber, String minHashKey,
			String maxHashKey) {
		super();
		this.ipAddress = ipAddress;
		this.portNumber = portNumber;
		this.minHashKey = minHashKey;
		this.maxHashKey = maxHashKey;
	}
	
	public NodeData(String name, String ipAddress, int portNumber, String minHashKey,
			String maxHashKey) {
		this(ipAddress, portNumber, minHashKey, maxHashKey);
		this.name = name;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public int getPortNumber() {
		return portNumber;
	}

	public String getMinHashKey() {
		return minHashKey;
	}

	public String getMaxHashKey() {
		return maxHashKey;
	}
	
	public String getName() {
		return name;
	}

	public void setMinHashKey(String minHashKey) {
		this.minHashKey = minHashKey;
	}

	public void setMaxHashKey(String maxHashKey) {
		this.maxHashKey = maxHashKey;
	}
	

}
