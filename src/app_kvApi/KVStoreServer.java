package app_kvApi;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.metadata.Address;
import common.metadata.MetadataHandlerServer;

public class KVStoreServer extends KVStore {

	public KVStoreServer(Address defaultAddress) {
		super(defaultAddress);
	}

	@Override
	protected void initMetadata(String defaultIp, int defaultPort) {
		this.metadataHandler = new MetadataHandlerServer(defaultIp, defaultPort);
		metadataHandler.update("node0;" + defaultIp + ";" + defaultPort + ";'';'';'';''");
	}

	@Override
	public KVMessage put(String key, String value) throws NoSuchAlgorithmException, UnknownHostException, IOException, InterruptedException {
		KVMessage answer = sendAndWaitAnswer(new KVMessageImpl(key, value,
				common.messages.KVMessage.StatusType.PUT));
		logger.debug("Answer to put : " + answer);
		return answer;
	}

	@Override
	public KVMessage get(String key) throws IOException, InterruptedException,
			NoSuchAlgorithmException {
		return null;
	}

	public void heartbeat(Address myAddress) throws IOException, InterruptedException {
		connect();
		sendAndWaitAnswer(new KVMessageImpl(myAddress.getIp(), Integer.toString(myAddress.getPort()),
				common.messages.KVMessage.StatusType.HEARTBEAT));
		disconnect();
	}

}
