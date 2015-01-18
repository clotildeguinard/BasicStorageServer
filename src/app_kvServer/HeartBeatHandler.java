package app_kvServer;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import common.messages.KVAdminMessageImpl;
import common.messages.KVMessage;
import common.metadata.Address;
import common.metadata.MetadataHandlerServer;
import common.metadata.NodeData;
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

			List<NodeData> neighbours = metadataHandler.getNeighbours();
			Address leftAddress = neighbours.get(0).getAddress();
			Address rightAddress = neighbours.get(1).getAddress();

			try {
				sendHeartbeats(leftAddress, rightAddress);
			} catch (IOException | InterruptedException e) {
				logger.error("An error occurred when sending heartbeats \n", e);
			}

			try {
				Thread.sleep(7500);
			} catch (InterruptedException e) {
				logger.warn("Interrupted when sleeping");
			}

			checkReceivedHeartbeats(leftAddress, rightAddress);

		}
		logger.debug("Heartbeat terminated");
	}

	private void checkReceivedHeartbeats(Address leftAddress,
			Address rightAddress) {
		boolean aliveL = false;
		boolean aliveR = false;

		while (receivedHeartbeats.size() > 0) {
			Address receivedHB = receivedHeartbeats.poll();
			if (receivedHB.isSameAddress(leftAddress)) {
				aliveL = true;
			} else if (receivedHB.isSameAddress(rightAddress)) {
				aliveR = true;
			}
		}

		if (aliveL && aliveR) {
			logger.debug("Both neighbours are alive.");
		} else {
			if (!aliveR) {
				logger.info("Right neighbour " + rightAddress + " is suspicious");
				ecsConnection.handleSuspNode(new KVAdminMessageImpl(rightAddress.getIp(),
						Integer.toString(rightAddress.getPort()),
						common.messages.KVAdminMessage.StatusType.SUSPICIOUS ));
			}
			if (!aliveL) {
				logger.info("Left neighbour " + leftAddress + " is suspicious");
				ecsConnection.handleSuspNode(new KVAdminMessageImpl(leftAddress.getIp(),
						Integer.toString(leftAddress.getPort()),
						common.messages.KVAdminMessage.StatusType.SUSPICIOUS ));
			}
		}
	}

	private void sendHeartbeats(Address leftAddress, Address rightAddress) throws IOException, InterruptedException {

		KVStoreServer kvStoreL = new KVStoreServer(leftAddress);
		KVStoreServer kvStoreR = new KVStoreServer(rightAddress);

		try {
			kvStoreL.heartbeat(metadataHandler.getMyAddress());
			logger.debug("Heartbeating left neighbour " + leftAddress);
		} catch (SocketException soe) {
			logger.warn("Socket exception when sending heartbeat  to " + leftAddress + "\n");
		}
		
		try {
			kvStoreR.heartbeat(metadataHandler.getMyAddress());
			logger.debug("Heartbeating right neighbour " + rightAddress);
		} catch (SocketException soe) {
			logger.warn("Socket exception when sending heartbeat  to " + rightAddress + "\n");
		}
		
	}

}
