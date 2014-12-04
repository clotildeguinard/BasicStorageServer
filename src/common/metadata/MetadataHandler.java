package common.metadata;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

public class MetadataHandler {
	private List<NodeData> metadata;
	private final static String fieldSeparator = ";";
	private final static String lineSeparator = "/";
	private final static String hashingAlgorithm = "MD5";
	private final static String equivLocalhost = "127.0.0.1";
	private final Logger logger = Logger.getLogger(getClass().getSimpleName());
	private String minHashKey;
	private String maxHashKey;
	private final String myIp;
	private final int myPort;

	/**
	 * used by server
	 * @param myIp
	 * @param myPort
	 */
	public MetadataHandler(String clientIp, int clientPort) {
		this.myIp = clientIp;
		this.myPort = clientPort;
	}
	
	/**
	 * used by ECS
	 * @param metadataContent
	 */
	public MetadataHandler(List<NodeData> metadata) {
		this.metadata = metadata;
		myPort = -1;
		myIp = null;
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
			if (equalsIp(this.myIp, ipAddress) && this.myPort == port) {
				minHashKey = data[3];
				maxHashKey = data[4];
			}
			NodeData nodeData = new NodeData(data[0], data[1], Integer.parseInt(data[2]), data[3], data[4]);
			tmpMetadata.add(nodeData);
		}
		this.metadata = tmpMetadata;	
	}

	private boolean equalsIp(String myIp, String ipAddress) {
		return (myIp.equals(ipAddress)
				|| (myIp.equals("localhost") && ipAddress.equals(equivLocalhost))
				|| (myIp.equals(equivLocalhost) && ipAddress.equals("localhost")));
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
		if (metadata.size() == 1) {
			return new String []
					{metadata.get(0).getIpAddress(), Integer.toString(metadata.get(0).getPortNumber())};
		}
		for (NodeData e: metadata){

			boolean b=isInRange(key, e.getMinHashKey(), e.getMaxHashKey());

			if (b==true){
				return new String []{e.getIpAddress(), Integer.toString(e.getPortNumber())};
			}

		}
		return null;
	}


	public boolean isResponsibleFor(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		boolean b;
		try {
			b = isInRange(key, minHashKey, maxHashKey);
		} catch (NoSuchAlgorithmException e) {
			Logger.getLogger(getClass().getSimpleName()).fatal("Hashing algorithm " + hashingAlgorithm + " could not be found!");
			throw(e);
		}
		return b;
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

	private boolean isInRange(String key, String minHash, String maxHash)
			throws NoSuchAlgorithmException, UnsupportedEncodingException {
		if (minHash.equals(maxHash)) {
			// i.e. there is only one node
			return true;
		}
		String hashedKey = new BigInteger(1,MessageDigest.getInstance(hashingAlgorithm)
				.digest(key.getBytes("UTF-8"))).toString(16);
		logger.debug("Hashed " + key + " --> " + hashedKey);
		try {
			if (minHash.compareTo(maxHash) <= 0) {
				return hashedKey.compareTo(minHash) > 0 && hashedKey.compareTo(maxHash) <= 0;
			} else {
				return hashedKey.compareTo(minHash) > 0 || hashedKey.compareTo(maxHash) <= 0;
			}	
		} catch (NullPointerException e) {
			logger.warn("Hash of key " + key + " was null!");
			return false;
		}
	}

	public boolean hasToMove(String key, String hashOfNewServer) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return !isInRange(key, minHashKey, hashOfNewServer);
	}

	public NodeData getRandom() throws NoSuchElementException {
		return metadata.get(0);
	}

}