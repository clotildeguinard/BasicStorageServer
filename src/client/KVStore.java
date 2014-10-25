package client;


import common.messages.KVMessage;

/**
 * send requests to the server, requires call to the defined communication protocol (encode, decode)
 * @author Clotilde
 *
 */
public class KVStore implements KVCommInterface {
	private final String address;
	private final int port;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}
	
	@Override
	public void connect() throws Exception {
		// copy and adapt from previous milestone
		
	}

	@Override
	public void disconnect() {
		// copy and adapt from previous milestone
		
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		//protocol defined by nadia to translate into a string, e.g.: "PUT KEY VALUE"
		//send this string with send() method copied and adapted from previous milestone
		// send() will call marshalling
		// then will call receive() method copied and adapted from previous milestone
		// unmarshall and return the corresponding KVMessage
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		//protocol defined by nadia to translate into a string, e.g.: "GET KEY"
		//send this string with send() method copied and adapted from previous milestone
		// send() will call marshalling
		// then will call receive() method copied and adapted from previous milestone
		// unmarshall and return the corresponding KVMessage
		return null;
	}

	
}
