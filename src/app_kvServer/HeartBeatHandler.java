package app_kvServer;

import java.io.IOException;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import common.messages.AdminMessageImpl;
import common.messages.KVMessage;
import common.metadata.Address;
import common.metadata.MetadataHandlerServer;
import app_kvApi.KVStoreServer;


public class HeartBeatHandler extends Thread {

	private static final Logger logger = Logger.getLogger(HeartBeatHandler.class);
	private boolean shutdown;
	private MetadataHandlerServer metadataHandler;
	private LinkedBlockingQueue<Address> receivedHeartbeats = new LinkedBlockingQueue<Address>();
	private EcsConnection ecsConnection;

	public HeartBeatHandler(MetadataHandlerServer metadataHandler, EcsConnection ecsConnection){
		this.metadataHandler = metadataHandler;
		this.ecsConnection = ecsConnection;
	}

	public void shutdown() {
		shutdown = true;
	}

	public void handleReceivedHeartBeat(KVMessage heartbeatMsg) {
		receivedHeartbeats.offer(new Address(heartbeatMsg.getKey(), Integer.valueOf(heartbeatMsg.getValue())));
	}

	@Override
	public void run() {

		while(!shutdown){		

			Set<Address> neighbours = metadataHandler.getNeighboursAddresses();
			
			try {
				sendHeartbeats(neighbours);
			} catch (IOException | InterruptedException e) {
				logger.error("An error occurred when sending heartbeats \n", e);
			}

			try {
				Thread.sleep(7500);
			} catch (InterruptedException e) {
				logger.warn("Interrupted when sleeping");
			}

			checkReceivedHeartbeats(neighbours);
		}
		logger.debug("Heartbeat terminated");
	}

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

	private void sendHeartbeats(Set<Address> neighbours) throws IOException, InterruptedException {

		for (Address a : neighbours) {
			KVStoreServer kvStore = new KVStoreServer(a);
			try {
				kvStore.heartbeat(metadataHandler.getMyAddress());
			} catch (SocketException soe) {
				logger.warn("Socket exception when sending heartbeat  to " + a + "\n");
			}
		}
	}

}