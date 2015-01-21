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
	protected List<NodeData> metadata;
	private final static String fieldSeparator = ";";
	private final static String lineSeparator = "/";
	private final static String hashingAlgorithm = "MD5";
	private final Logger logger = Logger.getLogger(MetadataHandler.class);
	protected List<NodeData> myNodeData = new ArrayList<>();
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
			NodeData nodeData = new NodeData(data[0], data[1], Integer.parseInt(data[2]),
					data[3], data[4], data[5], data[6]);
			if (myAddress.isSameAddress(a)) {
				myNodeData.add(nodeData);
			}
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
			String hashedKey = getHash(key);
			return hashIsInRange(hashedKey, minHash, maxHash);	
		} catch (NullPointerException e) {
			logger.warn("Hash of key " + key + " was null!");
			return false;
		} catch (NoSuchAlgorithmException e) {
			logger.fatal("Hashing algorithm " + hashingAlgorithm + " could not be found!");
			throw(e);
		}
	}
	
	protected String getHash(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return new BigInteger(1,MessageDigest.getInstance(hashingAlgorithm)
				.digest(key.getBytes("UTF-8"))).toString(16);
	}

	protected boolean hashIsInRange(String hashedKey, String minHash, String maxHash)
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
		int random = (int) (Math.random() * (metadata.size() - 1));
		return metadata.get(random);
	}


	/**
	 * @param address
	 * @return nodeData of virtual nodes corresponding to given address
	 */
	public List<NodeData> getNodeDataList(Address a) {
		List<NodeData> l = new ArrayList<NodeData>();
		for (NodeData e : metadata) {
			if (e.getAddress().isSameAddress(a)) {
				l.add(e);
			}
		}
		return l;
	}

}