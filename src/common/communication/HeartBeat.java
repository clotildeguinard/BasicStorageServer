package common.communication;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.KVMessage.StatusType;
import common.metadata.MetadataHandler;
import common.metadata.NodeData;
import app_kvClient.*;


public class HeartBeat extends Thread{

	private KVStore kvStore = new KVStore("localhost", 50000);
	private KVCommModule commModule;
	private static final Logger logger = Logger.getLogger(KVStore.class);
	private Socket clientSocket;
	private Set<KVSocketListener> listeners;
	private boolean stopped;
	private MetadataHandler metadataHandler;
	private static final int MAX_WAITING_MS = 3000;
	List<NodeData> neighbours;
	List<KVMessage> receivedHeartbeats;
	List<KVAdminMessage> suspiciousQueue;
	
	public HeartBeat(MetadataHandler metadataHandler, List heartbeatQueue, boolean stopped, List suspQueue){
		this.metadataHandler = metadataHandler;
		this.receivedHeartbeats = heartbeatQueue;
		this.stopped = stopped;
		this.suspiciousQueue = suspQueue;
	}
	
	@Override
	public void run() {
		
		neighbours = metadataHandler.getNeighbours();
		NodeData leftDude = neighbours.get(0);
		NodeData rightDude = neighbours.get(1);
		KVStore kvStoreL = new KVStore(leftDude.getIpAddress(), leftDude.getPortNumber());
		KVStore kvStoreR = new KVStore(rightDude.getIpAddress(), rightDude.getPortNumber());
		
		while(!stopped){			 
			 try {
				kvStoreL.heartbeat(metadataHandler.getIp(), metadataHandler.getPort());
				kvStoreR.heartbeat(metadataHandler.getIp(), metadataHandler.getPort());
				Thread.sleep(7500);
				if(receivedHeartbeats.size() == 2){
					receivedHeartbeats.remove(0);
					receivedHeartbeats.remove(0);
				} else if (receivedHeartbeats.size() == 1){
					KVMessage temp = receivedHeartbeats.remove(0);
					if(temp.getKey().equals(leftDude.getIpAddress()) && temp.getValue().equals(Integer.toString(leftDude.getPortNumber()))){
						suspiciousQueue.add(new KVAdminMessageImpl(rightDude.getIpAddress(), Integer.toString(rightDude.getPortNumber()), common.messages.KVAdminMessage.StatusType.SUSPICIOUS ));
					} else {
						suspiciousQueue.add(new KVAdminMessageImpl(leftDude.getIpAddress(), Integer.toString(leftDude.getPortNumber()), common.messages.KVAdminMessage.StatusType.SUSPICIOUS ));
					}
					
				}else if (receivedHeartbeats.size() == 0){
					suspiciousQueue.add(new KVAdminMessageImpl(rightDude.getIpAddress(), Integer.toString(rightDude.getPortNumber()), common.messages.KVAdminMessage.StatusType.SUSPICIOUS ));
					suspiciousQueue.add(new KVAdminMessageImpl(leftDude.getIpAddress(), Integer.toString(leftDude.getPortNumber()), common.messages.KVAdminMessage.StatusType.SUSPICIOUS ));

				}
				
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	

	
	
}
