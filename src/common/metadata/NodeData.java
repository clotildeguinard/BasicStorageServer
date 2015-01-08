package common.metadata;

public class NodeData {
	private String ipAddress;
	private int portNumber;
	private String maxHashKey;
	private String minHashKey;
	private String maxHashKey2;
	private String minReadHashKey;
	private String name;

	
	public NodeData(String name, String ipAddress, int portNumber, String minHashKey,
			String maxHashKey, String minReadHashKey, String maxHashKey2) {
		this.name = name;
		this.ipAddress = ipAddress;
		this.portNumber = portNumber;
		this.maxHashKey = maxHashKey;
		this.minHashKey = minHashKey;
		this.maxHashKey2 = maxHashKey2;
		this.minReadHashKey = minReadHashKey;
		
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
	
	public String getMaxHashKey2() {
		return maxHashKey2;
	}

	public String getMinReadHashKey() {
		return minReadHashKey;
	}

	public String getName() {
		return name;
	}
	

}
