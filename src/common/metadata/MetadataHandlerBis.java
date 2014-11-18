package common.metadata;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;

public class MetadataHandlerBis {
	private LinkedList<NodeData> metadata = new LinkedList<>();
	private final static String fieldSeparator = ";";
	private final static String lineSeparator = "/";
	private String minHashKey;
	private String maxHashKey;
	private final String ip;
	private final int port;
	
	
	public MetadataHandlerBis(String ip, int port) {
		this.ip = ip;
		this.port = port;
	}

	/**
	 * overwrite metadata file with more recent metadata
	 * @param metadata
	 * @param fileLocation
	 * @throws IOException
	 */
	public void update(String metadata) {
		LinkedList<NodeData> tmpMetadata = new LinkedList<>();
		String[] nodes = metadata.split(lineSeparator);
		String[] data = null;
		for (String n : nodes) {
			data = n.split(fieldSeparator);
			String ipAddress = data[0];
			int port = Integer.parseInt(data[1]);
			if (this.ip.equals(ipAddress) && this.port == port) {
				minHashKey = data[2];
				maxHashKey = data[3];
			}
			NodeData nodeData = new NodeData(data[0], Integer.parseInt(data[1]), data[2], data[3]);
			tmpMetadata.add(nodeData);
		}
		this.metadata = tmpMetadata;	
	}
	
	/**
	 * 
	 * @param fileLocation : the location of the metadata
	 * @param key : the key we are interested in
	 * @return array with server ip and port responsible for key
	 * @throws IOException
	 */
	public static String[] getServerForKey(String fileLocation, String key) {
		return null;

// TODO
	}

	public boolean isResponsibleFor(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return isInRange(key, minHashKey, maxHashKey);
	}

	public String toString() {
		StringBuilder s = new StringBuilder();
		for (NodeData n : metadata) {
			s.append(n.getIpAddress() + fieldSeparator);
			s.append(n.getPortNumber() + fieldSeparator);
			s.append(n.getMinHashKey() + fieldSeparator);
			s.append(n.getMaxHashKey() + lineSeparator);
		}
			return s.toString();
	}
	
	private boolean isInRange(String key, String minHash, String maxHash) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		String hashedKey = new BigInteger(1,MessageDigest.getInstance("MD5").digest(key.getBytes("UTF-8"))).toString(16);
		boolean b = hashedKey.compareTo(minHash) >= 0 && hashedKey.compareTo(maxHash) <= 0;
		if (minHash.compareTo(maxHash) <= 0) {
			return b;
		} else {
			return !b;
		}		
	}

	public boolean hasToMove(String key, String hashOfNewServer) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return !isInRange(key, minHashKey, hashOfNewServer);
	}

}
