package app_kvServer;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import app_kvApi.KVStoreServer;
import common.messages.AdminMessageImpl;
import common.messages.KVMessageImpl;
import common.metadata.Address;
import common.metadata.MetadataHandlerServer;

public class HeartBeatHandler extends Thread {

	private static final Logger logger = Logger.getLogger(HeartBeatHandler.class);
	private boolean stopHeartbeats;
	private MetadataHandlerServer metadataHandler;
	private LinkedBlockingQueue<Address> receivedHeartbeats = new LinkedBlockingQueue<Address>();
	private EcsConnection ecsConnection;

	public HeartBeatHandler(MetadataHandlerServer metadataHandler, EcsConnection ecsConnection){
		this.metadataHandler = metadataHandler;
		this.ecsConnection = ecsConnection;
	}

	public void shutdown() {
		stopHeartbeats = true;
	}

	public void handleReceivedHeartBeat(String senderIp, String senderPort) {
		try {
			receivedHeartbeats.offer(new Address(senderIp, Integer.parseInt(senderPort)));
		} catch (NumberFormatException e) {
			logger.warn("Received heartbeat could not be interpreted ! Received port value : " + senderPort);
		}
	}

	@Override
	public void run() {

		while(!stopHeartbeats){		

			Set<Address> neighbours = metadataHandler.getNeighboursAddresses();
			sendHeartbeats(neighbours);

			try {
				Thread.sleep(7500);
			} catch (InterruptedException e) {
				logger.warn("Interrupted when sleeping");
			}
			if (!stopHeartbeats) {
				checkReceivedHeartbeats(neighbours);
			}
		}
		logger.debug("Heartbeat terminated");
	}

	/**
	 * Read received heartbeat messages of previous round
	 * @param neighbours
	 */
	private void checkReceivedHeartbeats(Set<Address> neighbours) {

		while (receivedHeartbeats.size() > 0) {
			Address receivedHB = receivedHeartbeats.poll();
			neighbours.remove(receivedHB);
		}

		if (neighbours.isEmpty()) {
			logger.debug("All neighbours are alive.");

		} else {
			for (Address n : neighbours) {
				logger.info("Neighbour " + n + " is suspicious");
				ecsConnection.denounceSuspNode(new AdminMessageImpl(n.getIp(),
						Integer.toString(n.getPort()),
						common.messages.AdminMessage.StatusType.SUSPICIOUS ));
			}
		}
	}

	private void sendHeartbeats(Set<Address> neighbours) {

		for (Address neighbour : neighbours) {
			try {
				HeartbeatSender sender = new HeartbeatSender(neighbour);
				sender.heartbeat(metadataHandler.getMyAddress());
			} catch (IOException | InterruptedException e) {
				logger.warn(e.getMessage() + " when sending heartbeat to neighbour " + neighbour);
			}
		}
	}
	
	private class HeartbeatSender extends KVStoreServer {

		public HeartbeatSender(Address defaultAddress) {
			super(defaultAddress);
		}
		
		@Override
		protected void startListening() {
			// DO NOTHING
		}

		public void heartbeat(Address myAddress) throws IOException, InterruptedException {
			try {
				connect();
				sendAndWaitAnswer(new KVMessageImpl(myAddress.getIp(), Integer.toString(myAddress.getPort()),
						common.messages.KVMessage.StatusType.HEARTBEAT));
			} finally {
				disconnect();
				logger.debug("disconnected");
			}
		}
	}
}