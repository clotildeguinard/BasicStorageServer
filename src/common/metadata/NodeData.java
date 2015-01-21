package common.metadata;

import common.messages.KVMessage;

public class NodeData {
	private Address address;
	private String maxHashKey;
	private String minWriteHashKey;
	private String maxR2minR1HashKey;
	private String minR2HashKey;
	private String name;

	
	public NodeData(String name, String ipAddress, int portNumber,
			String maxHashKey, String minWriteHashKey, String maxR2minR1HashKey, String minR2HashKey) {
		this.name = name;
		this.address = new Address(ipAddress, portNumber);
		this.maxHashKey = maxHashKey;
		this.minWriteHashKey = minWriteHashKey;
		this.maxR2minR1HashKey = maxR2minR1HashKey;
		this.minR2HashKey = minR2HashKey;
		
	}

	public Address getAddress() {
		return address;
	}

	public String getMinWriteHashKey() {
		return minWriteHashKey;
	}

	public String getMaxHashKey() {
		return maxHashKey;
	}
	
	public String getMaxR2minR1HashKey() {
		return maxR2minR1HashKey;
	}

	public String getMinR2HashKey() {
		return minR2HashKey;
	}

	public String getName() {
		return name;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(name).append(";");
		sb.append(address).append(";");
		sb.append(maxHashKey).append(";");
		sb.append(minWriteHashKey).append(";");
		sb.append(maxR2minR1HashKey).append(";");
		sb.append(minR2HashKey);
		return sb.toString();
	}
	

}
