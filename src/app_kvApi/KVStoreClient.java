package app_kvApi;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.KVMessage.StatusType;
import common.metadata.Address;
import common.metadata.MetadataHandlerClient;

public class KVStoreClient extends KVStore {

	public KVStoreClient(String defaultIp, int defaultPort)
			throws IllegalArgumentException {
		super(defaultIp, defaultPort);
	}

	@Override
	protected void initMetadata(String defaultIp, int defaultPort) {
		this.metadataHandler = new MetadataHandlerClient(defaultIp, defaultPort);
		metadataHandler.update("node0;" + defaultIp + ";" + defaultPort + ";'';'';'';''");
	}

	@Override
	public KVMessage get(String key) throws InterruptedException,
	NoSuchAlgorithmException, UnsupportedEncodingException, UnknownHostException {
		List<Address> serversForKey = ((MetadataHandlerClient) metadataHandler).getReadServersForKey(key);
		KVMessage answer = null;
		if (serversForKey.size() == 0) {
			return getBis(null, key);
		} else {
			for (Address server : serversForKey) {
				answer = getBis(server, key);
				if (answer != null && !answer.getStatus().equals(StatusType.SERVER_STOPPED)) {
					return answer;
				}
			}
		}
		return answer;
	}

	private KVMessage getBis(Address server, String key)
			throws NoSuchAlgorithmException, UnknownHostException, InterruptedException {
		try  {
			if (server != null) {
				connect(server);
			} else {
				connect();
			}

			KVMessage answer = sendAndWaitAnswer(new KVMessageImpl(key, null,
					common.messages.KVMessage.StatusType.GET));
			disconnect();

			if (answer == null || answer.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
				return answer;
			}
			if (answer.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)) {
				metadataHandler.update(answer.getValue());
				return get(key);
			}

		} catch (IOException e) {
			logger.warn("IOException when requesting to server " + server + " : " + e.getMessage());
		}
		return null;
	}

	@Override
	public KVMessage put(String key, String value) throws InterruptedException,
	NoSuchAlgorithmException, UnknownHostException, UnsupportedEncodingException {
		KVMessage answer = putAndPropagate(key, value, 1);
		logger.debug("Answer to put : " + answer);
		return answer;
	}

	/**
	 * Send put request with "put and propagate to replicas" flag
	 * @param key
	 * @param value
	 * @param nbTrials
	 * @return Answer of server
	 * @throws NoSuchAlgorithmException
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws UnsupportedEncodingException 
	 */
	private KVMessage putAndPropagate(String key, String value, int nbTrials)
			throws NoSuchAlgorithmException, UnknownHostException,
			InterruptedException, UnsupportedEncodingException {
		
		if (nbTrials > MAX_TRIALS) {
			logger.warn("Responsible server for key " + key + " could not be found after "  + MAX_TRIALS + " trials.");
			return new KVMessageImpl(key, null, StatusType.PUT_ERROR);
		}

		Address serverForKey = ((MetadataHandlerClient) metadataHandler).getWriteServerForKey(key);
		try {
			if (serverForKey != null) {
				try {
					connect(serverForKey);
				} catch (IOException e) {
					logger.warn("IOException when connecting to server " + serverForKey + " : " + e.getMessage());
					connect();
				} 
			} else {
				connect();
			}

			KVMessage answer = sendAndWaitAnswer(new KVMessageImpl(key, value,
					common.messages.KVMessage.StatusType.PUT_PROPAGATE));

			disconnect();
			if (answer != null && answer.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)) {
				metadataHandler.update(answer.getValue());
				return putAndPropagate(key, value, nbTrials + 1);
			} else {
				return answer;
			}
		} catch (IOException e) {
			logger.warn("IOException when requesting to server : " + e.getMessage());
		}
		return null;
	}

}
