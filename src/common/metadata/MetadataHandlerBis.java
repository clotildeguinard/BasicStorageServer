package common.metadata;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

public class MetadataHandlerBis {
	private List<NodeDataBis> metadata;
	private final static String fieldSeparator = ";";
	private final static String lineSeparator = "/";
	private final static String hashingAlgorithm = "MD5";
	private final static String equivLocalhost = "127.0.0.1";
	private final Logger logger = Logger.getLogger(getClass().getSimpleName());
	private String minReadHashKey;
	private String maxHashKey2;
	private String minHashKey;
	private String maxHashKey;
	private final String myIp;
	private final int myPort;

	/**
	 * used by server
	 * @param myIp
	 * @param myPort
	 */
	public MetadataHandlerBis(String clientIp, int clientPort) {
		this.myIp = clientIp;
		this.myPort = clientPort;
	}

	/**
	 * used by ECS
	 * @param metadataContent
	 */
	public MetadataHandlerBis(List<NodeDataBis> metadata) {
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
		LinkedList<NodeDataBis> tmpMetadata = new LinkedList<>();
		String[] nodes = metadata.split(lineSeparator);
		String[] data = null;
		for (String n : nodes) {
			data = n.split(fieldSeparator);
			String ipAddress = data[1];
			int port = Integer.parseInt(data[2]);
			if (equalsIp(this.myIp, ipAddress) && this.myPort == port) {
				minHashKey = data[3];
				maxHashKey = data[4];
				minReadHashKey = data[5];
				maxHashKey2 = data[6];
			}
			NodeDataBis nodeData = new NodeDataBis(data[0], data[1], Integer.parseInt(data[2]), data[3], data[4], data[5], data[6]);
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

	public String[] getReadServerForKey(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		if (metadata.size() == 1) {
			return new String []
					{metadata.get(0).getIpAddress(), Integer.toString(metadata.get(0).getPortNumber())};
		}
		for (NodeDataBis e: metadata){

			boolean b=isInRange(key, e.getMinReadHashKey(), e.getMaxHashKey());

			if (b==true){
				return new String []{e.getIpAddress(), Integer.toString(e.getPortNumber())};
			}

		}
		return null;
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

	public String[] getWriteServerForKey(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		if (metadata.size() == 1) {
			return new String []
					{metadata.get(0).getIpAddress(), Integer.toString(metadata.get(0).getPortNumber())};
		}
		for (NodeDataBis e: metadata){

			boolean b=isInRange(key, e.getMinHashKey(), e.getMaxHashKey());

			if (b==true){
				return new String []{e.getIpAddress(), Integer.toString(e.getPortNumber())};
			}

		}
		return null;
	}


	public boolean isReadResponsibleFor(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return isInRange(key, minReadHashKey, maxHashKey);
	}

	public boolean isWriteResponsibleFor(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return isInRange(key, minHashKey, maxHashKey);
	}

	public String toString() {
		StringBuilder s = new StringBuilder();
		for (NodeDataBis n : metadata) {
			s.append(n.getName() + fieldSeparator);
			s.append(n.getIpAddress() + fieldSeparator);
			s.append(n.getPortNumber() + fieldSeparator);
			s.append(n.getMinHashKey() + fieldSeparator);
			s.append(n.getMaxHashKey() + lineSeparator);
		}
		return s.toString();
	}

	private boolean isInRange(String key, String minHash, String maxHash)
			throws UnsupportedEncodingException, NoSuchAlgorithmException {
		if (minHash.equals(maxHash)) {
			// i.e. there is only one node
			return true;
		}
		try {
			String hashedKey = new BigInteger(1,MessageDigest.getInstance(hashingAlgorithm)
					.digest(key.getBytes("UTF-8"))).toString(16);
			logger.debug("Hashed " + key + " --> " + hashedKey);

			return hashIsInRange(hashedKey, minHash, maxHash);	
		} catch (NullPointerException e) {
			logger.warn("Hash of key " + key + " was null!");
			return false;
		} catch (NoSuchAlgorithmException e) {
			Logger.getLogger(getClass().getSimpleName()).fatal("Hashing algorithm " + hashingAlgorithm + " could not be found!");
			throw(e);
		}
	}

	private boolean hashIsInRange(String hashedKey, String minHash, String maxHash)
			throws UnsupportedEncodingException, NoSuchAlgorithmException {
		if (minHash.equals(maxHash)) {
			// i.e. there is only one node
			return true;
		}
		if (minHash.compareTo(maxHash) <= 0) {
			return hashedKey.compareTo(minHash) > 0 && hashedKey.compareTo(maxHash) <= 0;
		} else {
			return hashedKey.compareTo(minHash) > 0 || hashedKey.compareTo(maxHash) <= 0;
		}
	}

	public boolean hasToMove(String key, String hashOfNewServer) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		if (hashIsInRange(hashOfNewServer, minHashKey, maxHashKey)) {
			return isInRange(key, minHashKey, hashOfNewServer);
		} else {
			return false;
		}
	}

	public boolean hasToDelete(String key, String hashOfNewServer) throws UnsupportedEncodingException, NoSuchAlgorithmException {
		if (hashIsInRange(hashOfNewServer, minReadHashKey, maxHashKey2)) {
			// can delete part of replica2 zone
			return isInRange(key, minReadHashKey, hashOfNewServer);
		} else if (hashIsInRange(hashOfNewServer, maxHashKey2, maxHashKey)) {
			// can delete whole replica2 zone
			return isInRange(key, minReadHashKey, maxHashKey2);
		} else {
			return false;
		}
	}

	public NodeDataBis getRandom() throws NoSuchElementException {
		return metadata.get(0);
	}

}