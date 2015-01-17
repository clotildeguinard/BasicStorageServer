package app_kvServer;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import common.communication.KVSocketListener;
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
	private boolean aliveL;
	private boolean aliveR;
	
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
			KVStoreServer kvStoreL = new KVStoreServer(leftAddress);
			KVStoreServer kvStoreR = new KVStoreServer(rightAddress);
			
			 try {
				logger.debug("New heartbeat round");
				logger.debug("Heartbeating left neighbour");
				kvStoreL.heartbeat(metadataHandler.getMyAddress());
				logger.debug("Heartbeating right neighbour");
				kvStoreR.heartbeat(metadataHandler.getMyAddress());
				Thread.sleep(7500);
				aliveL = false;
				aliveR = false;
				
				while (receivedHeartbeats.size() > 0) {
					Address receivedHB = receivedHeartbeats.poll();
					if (receivedHB.isSameAddress(leftAddress)) {
						aliveL = true;
					} else if (receivedHB.isSameAddress(rightAddress)) {
						aliveR = true;
					}
				}
				if (!aliveR) {
					logger.warn("Right neighbour " + rightAddress + " is suspicious");
					ecsConnection.handleSuspNode(new KVAdminMessageImpl(rightAddress.getIp(),
							Integer.toString(rightAddress.getPort()),
							common.messages.KVAdminMessage.StatusType.SUSPICIOUS ));
				}
				if (!aliveL) {
					logger.warn("Left neighbour " + leftAddress + " is suspicious");
					ecsConnection.handleSuspNode(new KVAdminMessageImpl(leftAddress.getIp(),
							Integer.toString(leftAddress.getPort()),
							common.messages.KVAdminMessage.StatusType.SUSPICIOUS ));
				}
				if (aliveL && aliveR) {
					logger.debug("Both neighbours are alive.");
				}
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		logger.debug("Heartbeat terminated");
	}
	
//	public static void main(String[] args) {
//		boolean i = true;
//		Cat c = new HeartBeat().new Cat(i);
//		System.out.println(c.le);
//		i=false;
//		System.out.println(c.le);
//	}
//	
//	private class Cat {
//		public boolean le;
//		public Cat(boolean legs){
//			le = legs;
//		}
//	}

	
	
}
