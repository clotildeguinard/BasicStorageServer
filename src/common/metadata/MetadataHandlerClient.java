package common.metadata;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

public class MetadataHandlerClient extends MetadataHandler {

	public MetadataHandlerClient(String clientIp, int clientPort) {
		super(clientIp, clientPort);
	}
	
	/**
	 * @param fileLocation : the location of the metadata
	 * @param key : the key we are interested in
	 * @return key-owner and key-replicas ip and port
	 * @throws UnsupportedEncodingException 
	 * @throws NoSuchAlgorithmException 
	 * @throws IOException
	 */
	public List<Address> getReadServersForKey(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		LinkedList<Address> servers = new LinkedList<>();
		for (NodeData e : metadata){
			if (isInRange(key, e.getMinR2HashKey(), e.getMaxHashKey())) {
				if (Math.random() >= 0.5) {
					servers.addFirst(e.getAddress());
				} else {
					servers.addLast(e.getAddress());
				}
			}
		}
		return servers;
	}

	/**
	 * @param fileLocation : the location of the metadata
	 * @param key : the key we are interested in
	 * @return key-owner ip and port
	 * @throws UnsupportedEncodingException 
	 * @throws NoSuchAlgorithmException 
	 * @throws IOException
	 */
	public Address getWriteServerForKey(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		for (NodeData e: metadata){

			boolean b=isInRange(key, e.getMinWriteHashKey(), e.getMaxHashKey());

			if (b==true){
				return e.getAddress();
			}

		}
		return null;
	}
}
