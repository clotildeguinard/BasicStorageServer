package common.metadata;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;


public class MetadataHandlerServer extends MetadataHandler {

	public MetadataHandlerServer(String clientIp, int clientPort) {
		super(clientIp, clientPort);
	}

	public Address getMyAddress(){
		return myAddress;		
	}
	
	/**
	 * Find left and right neighbours of current node
	 * @return
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
	 * @return true if server is owner or replica for this key, false otherwise
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
	 * @return true if server is owner for this key, false otherwise
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
	 * @return NodeData for replica1 of current node
	 */
	public NodeData getReplica1() {
		for (int i=0; i<metadata.size(); i++) {
			Address a = metadata.get(i).getAddress();
			if (myAddress.isSameAddress(a)) {
				return metadata.get((i-1 + metadata.size()) % metadata.size());
			}
		}
		return null;
	}

	/**
	 * @return NodeData for replica2 of current node
	 */
	public NodeData getReplica2() {
		for (int i=0; i<metadata.size(); i++) {
			Address a = metadata.get(i).getAddress();
			if (myAddress.isSameAddress(a)) {
				return metadata.get((i-2 + 2*metadata.size()) % metadata.size());
			}
		}
		return null;
	}


}
