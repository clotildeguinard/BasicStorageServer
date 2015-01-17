package common.metadata;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

public class MetadataHandler {
	protected List<NodeData> metadata;
	private final static String fieldSeparator = ";";
	private final static String lineSeparator = "/";
	private final static String hashingAlgorithm = "MD5";
	private final Logger logger = Logger.getLogger(getClass().getSimpleName());
	protected String minR2HashKey;
	protected String maxR2minR1HashKey;
	protected String minWriteHashKey;
	protected String maxHashKey;
	protected final Address myAddress;

	/**
	 * @param myIp
	 * @param myPort
	 */
	public MetadataHandler(String myIp, int myPort) {
		this.myAddress = new Address(myIp, myPort);
	}

	/**
	 * @param metadataContent
	 */
	public MetadataHandler(List<NodeData> metadata) {
		this.metadata = metadata;
		this.myAddress = new Address(null, -1);
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
			if (data.length != 7) {
				logger.warn("Tried to update metadata with invalid input " + metadata);
				return;
			}
			Address a = new Address(data[1], Integer.parseInt(data[2]));
			if (myAddress.isSameAddress(a)) {
				maxHashKey = data[3];
				minWriteHashKey = data[4];
				maxR2minR1HashKey = data[5];
				minR2HashKey = data[6];
			}
			NodeData nodeData = new NodeData(data[0], data[1], Integer.parseInt(data[2]), data[3], data[4], data[5], data[6]);
			tmpMetadata.add(nodeData);
		}
		this.metadata = tmpMetadata;	
	}

	public String toString() {
		StringBuilder s = new StringBuilder();
		for (NodeData n : metadata) {
			Address a = n.getAddress();
			
			s.append(n.getName() + fieldSeparator);
			s.append(a.getIp() + fieldSeparator);
			s.append(a.getPort() + fieldSeparator);
			s.append(n.getMaxHashKey() + fieldSeparator);
			s.append(n.getMinWriteHashKey() + fieldSeparator);
			s.append(n.getMaxR2minR1HashKey() + fieldSeparator);
			s.append(n.getMinR2HashKey() + lineSeparator);
		}
		return s.toString();
	}

	/**
	 * used locally (kvstore and server) and by server
	 * @param key
	 * @param minHash
	 * @param maxHash
	 * @return
	 * @throws UnsupportedEncodingException
	 * @throws NoSuchAlgorithmException
	 */
	public boolean isInRange(String key, String minHash, String maxHash)
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

	public NodeData getRandom() throws NoSuchElementException {
		return metadata.get(0);
	}


	/**
	 * used by ECS
	 * @param ip
	 * @param portNumber
	 * @return
	 */
	public NodeData getNodeData(String ip, int portNumber) {
		Address a = new Address(ip, portNumber);
		for (NodeData e : metadata) {
			if (e.getAddress().isSameAddress(a)) {
				return e;
			}
		}
		return null;
	}

}