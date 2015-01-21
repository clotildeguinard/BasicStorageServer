package common.metadata;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MetadataHandlerServer extends MetadataHandler {

	public MetadataHandlerServer(String clientIp, int clientPort) {
		super(clientIp, clientPort);
	}

	public Address getMyAddress(){
		return myAddress;		
	}

	/**
	 * @return all left and right neighbours of current node's virtual nodes
	 */
	public Set<Address> getNeighboursAddresses(){

		Set<Address> neighbours = new HashSet<>();
		int MAX = metadata.size();
		for (int i = 0; i < MAX; i++) {
			Address a = metadata.get(i).getAddress();
			if (myAddress.isSameAddress(a)) {
				neighbours.add(metadata.get((i + 1) % MAX).getAddress());
				neighbours.add(metadata.get((i - 1 + MAX) % MAX).getAddress());
			}
		}
		neighbours.remove(myAddress);
		return neighbours;		
	}

	/**
	 * @param key
	 * @return true if one of the virtual nodes is owner or replica for this key, false otherwise
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 */
	public boolean isReadResponsibleFor(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		for (NodeData n : myNodeData) {
			if (isInRange(key, n.getMinR2HashKey(), n.getMaxHashKey())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * @param key
	 * @return true if one of the virtual nodes is owner for this key
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 */
	public boolean isWriteResponsibleFor(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		for (NodeData n : myNodeData) {
			if (isInRange(key, n.getMinWriteHashKey(), n.getMaxHashKey())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 
	 * @param hashKey
	 * @return list of virtual nodes being read-responsible for the key, excluding those corresponding to same server
	 * @throws UnsupportedEncodingException
	 * @throws NoSuchAlgorithmException
	 */
	public List<NodeData> getReplicas(String key) 
			throws UnsupportedEncodingException, NoSuchAlgorithmException {
		List<NodeData> l = new ArrayList<NodeData>();
		String hash = getHash(key);
		for (NodeData n : metadata) {
			if (!n.getAddress().isSameAddress(myAddress)
					&& hashIsInRange(hash, n.getMinR2HashKey(), n.getMinWriteHashKey())) {
				l.add(n);
			}
		}
		return l;
	}

	/**
	 * 
	 * @param hashKey
	 * @return virtual node being replica1 for the key
	 * @throws UnsupportedEncodingException
	 * @throws NoSuchAlgorithmException
	 */
	public NodeData getReplica1(String key) 
			throws UnsupportedEncodingException, NoSuchAlgorithmException {
		String hash = getHash(key);
		for (NodeData n : metadata) {
			if (hashIsInRange(hash, n.getMaxR2minR1HashKey(), n.getMinWriteHashKey())) {
				return n;
			}
		}
		return null;
	}

	/**
	 * 
	 * @param hashKey
	 * @return virtual node being replica2 for the key
	 * @throws UnsupportedEncodingException
	 * @throws NoSuchAlgorithmException
	 */
	public NodeData getReplica2(String key) 
			throws UnsupportedEncodingException, NoSuchAlgorithmException {
		String hash = getHash(key);
		for (NodeData n : metadata) {
			if (hashIsInRange(hash, n.getMinR2HashKey(), n.getMaxR2minR1HashKey())) {
				return n;
			}
		}
		return null;
	}
}