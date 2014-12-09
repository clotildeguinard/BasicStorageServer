package common.metadata;

public class NodeData {
	private String ipAddress;
	private int portNumber;
	private String minHashKey;
	private String maxHashKey;
	private String name;

	
	public NodeData(String name, String ipAddress, int portNumber, String minHashKey,
			String maxHashKey) {
		this.name = name;
		this.ipAddress = ipAddress;
		this.portNumber = portNumber;
		this.minHashKey = minHashKey;
		this.maxHashKey = maxHashKey;
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
	

}
