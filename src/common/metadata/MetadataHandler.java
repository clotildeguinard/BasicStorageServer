package common.metadata;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public class MetadataHandler {
	private LinkedList<NodeData> metadata = new LinkedList<>();
	private final static String fieldSeparator = ";";
	private final static String lineSeparator = "/";
	private String minHashKey;
	private String maxHashKey;
	private final String clientIp;
	private final int clientPort;


	public MetadataHandler(String clientIp, int clientPort) {
		this.clientIp = clientIp;
		this.clientPort = clientPort;
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
			String ipAddress = data[1];
			int port = Integer.parseInt(data[2]);
			if (this.clientIp.equals(ipAddress) && this.clientPort == port) {
				minHashKey = data[3];
				maxHashKey = data[4];
			}
			NodeData nodeData = new NodeData(data[0], data[1], Integer.parseInt(data[2]), data[3], data[4]);
			tmpMetadata.add(nodeData);
		}
		this.metadata = tmpMetadata;	
	}

	/**
	 * 
	 * @param fileLocation : the location of the metadata
	 * @param key : the key we are interested in
	 * @return array with server ip and port responsible for key
	 * @throws UnsupportedEncodingException 
	 * @throws NoSuchAlgorithmException 
	 * @throws IOException
	 */

	public String[] getServerForKey(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {

		for (NodeData e: metadata){

			boolean b=isInRange(key, e.getMinHashKey(), e.getMaxHashKey());

			if (b==true){
				return new String []{e.getIpAddress(), Integer.toString(e.getPortNumber())
				};
			}

		}
		return null;
	}


	public boolean isResponsibleFor(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return isInRange(key, minHashKey, maxHashKey);
	}

	public String toString() {
		StringBuilder s = new StringBuilder();
		for (NodeData n : metadata) {
			s.append(n.getName() + fieldSeparator);
			s.append(n.getIpAddress() + fieldSeparator);
			s.append(n.getPortNumber() + fieldSeparator);
			s.append(n.getMinHashKey() + fieldSeparator);
			s.append(n.getMaxHashKey() + lineSeparator);
		}
		return s.toString();
	}

	private boolean isInRange(String key, String minHash, String maxHash) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		String hashedKey = new BigInteger(1,MessageDigest.getInstance("MD5").digest(key.getBytes("UTF-8"))).toString(16);
		try {
			boolean b = hashedKey.compareTo(minHash) >= 0 && hashedKey.compareTo(maxHash) <= 0;
			if (minHash.compareTo(maxHash) <= 0) {
				return b;
			} else {
				return !b;
			}	
		} catch (NullPointerException e) {
			return false;
		}
	}

	public boolean hasToMove(String key, String hashOfNewServer) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return !isInRange(key, minHashKey, hashOfNewServer);
	}

	public NodeData getRandom() throws NoSuchElementException {
		return metadata.getFirst();
	}

}