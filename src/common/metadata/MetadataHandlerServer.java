package common.metadata;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;


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
	public List<NodeData> getNeighbours(){

		List<NodeData> neighbours = new ArrayList<>(2);
		int MAX = metadata.size();
		for (int i = 0; i < MAX; i++) {
			Address a = metadata.get(i).getAddress();
			if (myAddress.isSameAddress(a)) {
				neighbours.add(metadata.get((i + 1) % MAX));
				neighbours.add(metadata.get((i - 1 + MAX) % MAX));
			}
		}
		return neighbours;		
	}
	
	/**
	 * @param key
	 * @return true if server is owner or replica for this key, false otherwise
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 */
	public boolean isReadResponsibleFor(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return isInRange(key, minR2HashKey, maxHashKey);
	}

	/**
	 * @param key
	 * @return true if server is owner for this key, false otherwise
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 */
	public boolean isWriteResponsibleFor(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return isInRange(key, minWriteHashKey, maxHashKey);
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
