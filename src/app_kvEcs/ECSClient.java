package app_kvEcs;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.cache_strategies.Strategy;
import common.communication.SocketListener;
import common.messages.AdminMessage;
import common.messages.AdminMessageImpl;
import common.messages.TextMessage;
import common.metadata.Address;
import common.metadata.MetadataHandler;
import common.metadata.NodeData;

public class ECSClient extends ECSUtils implements SocketListener {
	private static final Logger logger = Logger.getLogger(ECSClient.class);
	private final static String PROMPT = "ECSClient> ";

	private boolean suspicionHandlingLocked = false;
	private HashMap<Address, Long> suspicionMap = new HashMap<>();

	private int defaultCacheSize;
	private Strategy defaultDisplacementStrategy;
	private boolean poolStarted = false;

	private int nbUsedNodes = 0;
	private MetadataHandler metadataHandler;

	public ECSClient(String configLocation) {
		readConfigFile(configLocation);
	}

	/**
	 * Initiate connection to a pool of nodes and heartbeat between them
	 * @param numberOfNodes
	 * @param cacheSize
	 * @param displacementStrategy
	 * @return number of nodes participating in the service
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 */
	public int initService(int numberOfNodes, int cacheSize, Strategy displacementStrategy)
			throws NoSuchAlgorithmException, IOException {
		this.defaultCacheSize = cacheSize;
		this.defaultDisplacementStrategy = displacementStrategy;

		if (numberOfNodes > possibleRemainingNodes.size()) {
			numberOfNodes = possibleRemainingNodes.size();
		}

		logger.debug("Initiating the service...");
		sortedConfigStores = new LinkedList<ConfigCommInterface>();
		uniqueConfigStores = new ArrayList<ConfigCommInterface>();
		sortedNodeHashes = new LinkedList<String>();

		for (int k = 0; k < numberOfNodes ; k++) {
			String[] newNodeParams = possibleRemainingNodes.get(0);
			String nodeName = newNodeParams[0];
			String ip = newNodeParams[1];
			int port = Integer.valueOf(newNodeParams[2]);

			addNodeToLists(nodeName, ip, port);
		}
		sortNodeLists();
		mergeAdjacentVirtualNodes();

		metadataHandler = new MetadataHandler(getMetadataFromSortedList());
		String metadata = metadataHandler.toString();
		for (ConfigCommInterface cs : uniqueConfigStores) {
			((ConfigStore) cs).connect();
			((ConfigStore) cs).addListener(this);
			cs.initKVServer(metadata, cacheSize, displacementStrategy);
		}


		logger.info("Remaining " + possibleRemainingNodes.size() + " unused nodes : \n" + printPossible());
		logger.info(uniqueConfigStores.size() + " used nodes with " + sortedConfigStores.size() + " virtual nodes : \n" + printNodes());


		startHeartbeats();
		nbUsedNodes = numberOfNodes;
		return nbUsedNodes;
	}

	/**
	 * Start all nodes participating in the service
	 * i.e. disable answer to queries from clients
	 */
	protected void start(){
		logger.info("Starting all used nodes.");
		for (ConfigCommInterface cs : uniqueConfigStores) {
			if (!cs.startServer()) {
				logger.error("Server " + cs.getServerAddress() + " may not have been"
						+ " started correctly.");
			}
		}
		poolStarted = true;
	}

	/**
	 * Stop all nodes participating in the service
	 * i.e. disable answer to queries from clients
	 */
	protected void stop(){
		if (uniqueConfigStores == null) {
			return;
		}
		logger.info("Stopping all used nodes.");
		for (ConfigCommInterface cs : uniqueConfigStores) {
			if (!cs.stopServer()) {
				logger.error("Server " + cs.getServerAddress() + " may not have been"
						+ " stopped correctly.");
			}
		}
		poolStarted = false;
	}

	/**
	 * Shutdown all nodes participating in the service
	 * i.e. stop them, stop heartbeat and close connection to them
	 */
	protected void shutdown(){
		if (uniqueConfigStores == null) {
			return;
		}
		logger.info("Shutting down all used nodes.");
		for (ConfigCommInterface cs : uniqueConfigStores) {
			if (!cs.shutdown()) {
				logger.error("Server " + cs.getServerAddress() + " may not have been"
						+ " shut down correctly.");
			}
			cs = null;
		}
		sortedConfigStores = null;
		sortedNodeHashes = null;
		uniqueConfigStores = null;

		nbUsedNodes = 0;
		poolStarted = false;
	}

	/**
	 * Stop heartbeat for all nodes participating in the service
	 * i.e. disable sending heartbeats and denouncing suspicious nodes
	 */
	protected void stopHeartbeats(){
		suspicionHandlingLocked = true;
		suspicionMap.clear();
		logger.info("Stopping heartbeat on all used nodes");
		for (ConfigCommInterface cs : uniqueConfigStores) {
			if (!cs.stopHeartbeat()) {
				logger.error("Server " + cs.getServerAddress() + " may not have been"
						+ " stopped to heartbeat correctly.");
			}
		}
	}

	/**
	 * Start heartbeat for all nodes participating in the service
	 * i.e. enable sending heartbeats and denouncing suspicious nodes
	 */
	protected void startHeartbeats(){
		suspicionHandlingLocked = false;
		logger.info("Starting heartbeat on all used nodes.");
		for (ConfigCommInterface cs : uniqueConfigStores) {
			if (!cs.startHeartbeat()) {
				logger.error("Server " + cs.getServerAddress() + " may not have been"
						+ " started to heartbeat correctly.");
			}
		}
	}

	/**
	 * Send updated metadata to all nodes participating in the service
	 */
	protected void updateAllMetadata() {
		String updatedMetadata = metadataHandler.toString();
		for (ConfigCommInterface c : uniqueConfigStores) {
			try {
				if (!c.updateMetadata(updatedMetadata)) {
					logger.error("The metadata may not have been updated correctly"
							+ " on server " + ((ConfigStore) c).getServerAddress());
				}
			} catch (InterruptedException | IOException e) {
				logger.error("The metadata may not have been updated correctly"
						+ " on server " + ((ConfigStore) c).getServerAddress());
			}
		}
		logger.info("Metadata updated for all nodes");
	}

	/**
	 * Add node to the pool of participating nodes
	 * @param cacheSize
	 * @param displacementStrategy
	 * @return number of nodes participating in the service
	 * @throws NoSuchElementException
	 * @throws NoSuchAlgorithmException
	 * @throws IOException 
	 */
	// TODO check
	protected int addNode(int cacheSize, Strategy displacementStrategy)
			throws NoSuchElementException, NoSuchAlgorithmException, IOException{

		int size = possibleRemainingNodes.size(); 
		if (size < 1) {
			throw new NoSuchElementException("There is no more node to add.");
		}

		stopHeartbeats();

		try {
			int random = (int) (Math.random() * (size - 1));
			String[] newNodeParams = possibleRemainingNodes.get(random);
			String nodeName = newNodeParams[0];
			String ip = newNodeParams[1];
			int port = Integer.valueOf(newNodeParams[2]);
			Address addedNodeAddress = new Address(ip, port);

			logger.debug("Adding node " + addedNodeAddress);

			try {
				addNodeToLists(nodeName, ip, port);
			} catch (IllegalArgumentException e1) {
				possibleRemainingNodes.add(newNodeParams);
				logger.error("ECS config file may contain an error", e1);
				throw(e1);
			} catch (IOException e1) {
				possibleRemainingNodes.add(newNodeParams);
				logger.error("Could not add node", e1);
				throw(e1);
			}
			sortNodeLists();
			mergeAdjacentVirtualNodes();

			ConfigCommInterface newNodeCS = uniqueConfigStores.get(uniqueConfigStores.size()-1);
			((ConfigStore) newNodeCS).connect();
			((ConfigStore) newNodeCS).addListener(this);

			metadataHandler = new MetadataHandler(getMetadataFromSortedList());
			String updatedMetadata = metadataHandler.toString();
			if (!newNodeCS.initKVServer(updatedMetadata, cacheSize, displacementStrategy)) {
				logger.error("The new node may not have been initialized correctly.");
			} else {
				logger.info("New node initialized.");
			}
			if (poolStarted) {
				if (!newNodeCS.startServer()) {
					logger.error("The new node may not have been started correctly.");
				} else {
					logger.info("New node started.");
				}
			}

			Set<ConfigCommInterface> toUnlock = new HashSet<ConfigCommInterface>();
			Address newNodeAddress = newNodeCS.getServerAddress();
			List<Integer> indexes = findVirtualNodeIndexes(newNodeAddress);
			List<NodeData> newVNodeDataList = metadataHandler.getNodeDataList(newNodeAddress);

			int k = 0;
			int MAX = sortedConfigStores.size();
			for (NodeData newNodeData : newVNodeDataList) {
				int index = indexes.get(k);
				ConfigCommInterface nextPhysNodeCS = sortedConfigStores.get((index + 1) % MAX);
				ConfigCommInterface nextnextPhysNodeCS = sortedConfigStores.get((index + 2) % MAX);
				if (nextnextPhysNodeCS.getServerAddress().isSameAddress(newNodeAddress)) {
					nextnextPhysNodeCS = sortedConfigStores.get((index + 3) % MAX);
				}

				redistributeDataAdd(newNodeAddress, newNodeData, nextPhysNodeCS, nextnextPhysNodeCS);

				toUnlock.add(nextPhysNodeCS);
				toUnlock.add(nextnextPhysNodeCS);
				k++;
			}

			updateAllMetadata();

			for (ConfigCommInterface cs : toUnlock) {
				if (!cs.unlockWrite()) {
					logger.error("Node " + cs.getServerAddress() + " may not have been write-unlocked correctly.");
				} else {
					logger.debug("Node " + cs.getServerAddress() + " write-unlocked.");
				}
			}

			logger.info("Remaining " + possibleRemainingNodes.size() + " unused nodes : \n" + printPossible());
			logger.info(uniqueConfigStores.size() + " used nodes with " + sortedConfigStores.size() + " virtual nodes : \n" + printNodes());


			return ++nbUsedNodes; 

		} finally {
			startHeartbeats();
		}
	}

	/**
	 * COPY owner range from SUCCESSOR to NEW
	 * MOVE r2 range from SUCCESSOR to NEW
	 * MOVE r1 range from AFTER-SUCCESSOR to NEW
	 * @param newNodeAddress
	 * @param newNodeData
	 * @param nextNodeCS
	 * @param nextnextNodeCS
	 */
	private void redistributeDataAdd(Address newNodeAddress, NodeData newNodeData, ConfigCommInterface nextNodeCS, ConfigCommInterface nextnextNodeCS) {

		if (!nextNodeCS.lockWrite()) {
			logger.error("The successor node may not have been write-locked correctly.");
		} else {
			logger.debug("Successor node write-locked.");
		}

		// COPY owner range from NEXT to NEW
		if (!nextNodeCS.copyData(newNodeAddress, newNodeData.getMinWriteHashKey(), newNodeData.getMaxHashKey())) {
			logger.error("The owner data may not have been copied correctly.");
		} else {
			logger.info("Owner data copied to new node.");
		}

		// MOVE r2 range from NEXT to NEW
		if (!nextNodeCS.moveData(newNodeAddress, newNodeData.getMinR2HashKey(), newNodeData.getMaxR2minR1HashKey())) {
			logger.error("The R2 data may not have been moved correctly.");
		} else {
			logger.info("R2 data moved to new node.");
		}

		if (!nextnextNodeCS.lockWrite()) {
			logger.error("The after-successor node may not have been write-locked correctly.");
		} else {
			logger.info("After-successor node write-locked.");
		}

		// MOVE r1 range from NEXT-NEXT to NEW
		if (!nextnextNodeCS.moveData(newNodeAddress, newNodeData.getMaxR2minR1HashKey(), newNodeData.getMinWriteHashKey())) {
			logger.error("The R1 data may not have been moved correctly.");
		} else {
			logger.info("R1 data moved to new node.");
		}
	}

	/**
	 * Remove node from the pool of participating nodes
	 * @return number of participating nodes
	 * @throws IOException
	 */
	protected int removeNode() throws IOException, IllegalArgumentException {
		if (nbUsedNodes <= 3) {
			throw new IllegalArgumentException();
		}

		try {
			stopHeartbeats();

			int toRemoveIndex = (int) (Math.random() * (uniqueConfigStores.size() - 1));
			ConfigCommInterface removedNodeCS = uniqueConfigStores.get(toRemoveIndex);
			Address removedAddress = removedNodeCS.getServerAddress();

			logger.info("Removing node " + removedAddress);

			List<Integer> indexesOfSuccessors = removeNodeFromLists(removedAddress);
			List<NodeData> removedVNList = metadataHandler.getNodeDataList(removedAddress);
			if (removedVNList.size() != indexesOfSuccessors.size()) {
				logger.fatal("problem!!!");
			}

			metadataHandler = new MetadataHandler(getMetadataFromSortedList());

			removedNodeCS.lockWrite();

			int k = 0;
			for (NodeData removedVN : removedVNList) {
				redistributeDataRemove(indexesOfSuccessors.get(k), removedVN, removedNodeCS);
				k++;
			}

			if (!removedNodeCS.shutdown()) {
				logger.error("The removed node may not have been shut down correctly.");
			}

			mergeAdjacentVirtualNodes();
			updateAllMetadata();

			logger.debug("Possible nodes : \n" + printPossible());
			logger.debug("Used nodes : \n" + printNodes());

			return --nbUsedNodes;

		} finally {
			startHeartbeats();
			logger.info("Remaining " + possibleRemainingNodes.size() + " unused nodes : \n" + printPossible());
			logger.info(uniqueConfigStores.size() + " used nodes with " + sortedConfigStores.size() + " virtual nodes : \n" + printNodes());

		}
	}

	/**
	 * COPY r2 range from TOREMOVE to PHYSICAL SUCCESSOR
	 * COPY r1 range from TOREMOVE to PHYSICAL AFTER-SUCCESSOR
	 * @param removedIndex
	 * @param removedNodeData
	 * @param toRemoveNodeCS
	 * @throws IOException
	 */
	private void redistributeDataRemove(int removedIndex, NodeData removedNodeData, ConfigCommInterface toRemoveNodeCS) throws IOException {		
		ConfigCommInterface nextPhysNodeCS = sortedConfigStores.get(removedIndex);
		ConfigCommInterface nextnextPhysNodeCS = sortedConfigStores.get((removedIndex + 1) % sortedConfigStores.size());
		String newMetadata = metadataHandler.toString();

		try {
			if (!nextPhysNodeCS.updateMetadata(newMetadata)) {
				logger.error("The metadata may not have been updated correctly on successor node " + nextPhysNodeCS.getServerAddress());
			} else {
				logger.info("Metadata updated on successor node " + nextPhysNodeCS.getServerAddress());
			}
		} catch (InterruptedException e) {
			logger.error("The metadata may not have been updated correctly on successor node " + nextPhysNodeCS.getServerAddress());
		}

		// COPY r2 range from TOREMOVE to NEXT PHYS

		if (!toRemoveNodeCS.copyData(nextPhysNodeCS.getServerAddress(),
				removedNodeData.getMinR2HashKey(), removedNodeData.getMaxR2minR1HashKey())) {
			logger.error("The R2 data may not have been copied correctly from removed node " + toRemoveNodeCS.getServerAddress() + " to successor node.");
		} else {
			logger.info("R2 data copied from removed node " + toRemoveNodeCS.getServerAddress() + " to successor node.");
		}

		try {
			if (!nextnextPhysNodeCS.updateMetadata(newMetadata)) {
				logger.error("The metadata may not have been updated correctly on after-successor node " + nextnextPhysNodeCS.getServerAddress());
			} else {
				logger.info("Metadata updated on after-successor node " + nextnextPhysNodeCS.getServerAddress());
			}

		} catch (InterruptedException e) {
			logger.error("The metadata may not have been updated correctly on after-successor node " + nextnextPhysNodeCS.getServerAddress());
		}

		// COPY r1 range from TOREMOVE to NEXT-NEXT PHYS
		if (!toRemoveNodeCS.copyData(nextnextPhysNodeCS.getServerAddress(),
				removedNodeData.getMaxR2minR1HashKey(), removedNodeData.getMinWriteHashKey())) {
			logger.error("The R1 data may not have been copied correctly from removed node " + toRemoveNodeCS.getServerAddress() + " to after-successor node.");
		} else {
			logger.info("R1 data copied from removed node " + toRemoveNodeCS.getServerAddress() + " to after-successor node.");
		}
	}

	/**
	 * Handles suspicious node
	 * if is not being handled
	 * and if is reported by the two neighbours
	 * @param ip
	 * @param port
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 */
	protected void handleSuspiciousNode(String ip, int port) throws NoSuchAlgorithmException, IOException{

		if (suspicionHandlingLocked) {
			return;

		} else {
			Address a = new Address(ip, port);
			long time = System.currentTimeMillis();
			synchronized(this) {
				if (suspicionMap.containsKey(a) && (time - suspicionMap.get(a) < 7450) ) {
					logger.info("----------------------------" + "\n"
							+ "Has to handle suspicious node " + a
							+ "\n" + "----------------------------");
					removeSuspiciousNode(ip, port);
					suspicionMap.clear();
				} else if (System.currentTimeMillis() - time < 1000){
					suspicionMap.put(a, time);
				}
			}
		}
	}

	/**
	 * Remove suspicious node from the pool of participating nodes,
	 * bring the data back to consistency by copying it from replicas
	 * and re-add random node
	 * @param ip
	 * @param port
	 * @return number of used nodes
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 */
	protected void removeSuspiciousNode(String ip, int port)
			throws NoSuchAlgorithmException, IOException {

		Address suspAddress = new Address(ip, port);
		ConfigCommInterface suspNodeCS = null;
		for (ConfigCommInterface cs : uniqueConfigStores) {
			if (cs.getServerAddress().isSameAddress(suspAddress)) {
				suspNodeCS = cs;
				break;
			}
		}

		List<NodeData> suspNodeDataList = metadataHandler.getNodeDataList(new Address(ip, port));
		List<Integer> indexesOfSuccessors = removeNodeFromLists(suspAddress);
		if (suspNodeDataList.size() != indexesOfSuccessors.size()) {
			logger.fatal("problem!!!!!");
		}

		stopHeartbeats();

		metadataHandler = new MetadataHandler(getMetadataFromSortedList());

		suspNodeCS.lockWrite();

		try {

			Set<ConfigCommInterface> toUnlock = new HashSet<ConfigCommInterface>();
			int k=0;
			for (NodeData suspNode : suspNodeDataList) {

				int suspIndex = indexesOfSuccessors.get(k);
				int MAX = sortedConfigStores.size();

				ConfigCommInterface previousNodeCS = sortedConfigStores.get((suspIndex - 1 + MAX) % MAX);
				redistributeDataSuspicious(suspIndex, suspNode, previousNodeCS);
				toUnlock.add(previousNodeCS);

				k++;
			}

			if (!suspNodeCS.shutdown()) {
				logger.error("The removed node may not have been shut down correctly.");
			}

			mergeAdjacentVirtualNodes();
			updateAllMetadata();

			for (ConfigCommInterface cs : toUnlock) {
				if (!cs.unlockWrite()) {
					logger.error("Node " + cs.getServerAddress() + " may not have been write-unlocked correctly.");
				} else {
					logger.debug("Node " + cs.getServerAddress() + " write-unlocked.");
				}
			}

			logger.debug("Possible nodes : \n" + printPossible());
			logger.debug("Used nodes : \n" + printNodes());

			addNode(defaultCacheSize, defaultDisplacementStrategy);
		} finally {
			startHeartbeats();
			logger.info("Remaining " + possibleRemainingNodes.size() + " unused nodes : \n" + printPossible());
			logger.info(uniqueConfigStores.size() + " used nodes with " + sortedConfigStores.size() + " virtual nodes : \n" + printNodes());

		}
	}

	/**
	 * COPY r2 range of SUSP from PHYSICAL PREVIOUS to PHYSICAL SUCCESSOR
	 * COPY r1 range of SUSP from PHYSICAL PREVIOUS to PHYSICAL AFTER-SUCCESSOR
	 * @param suspIndex
	 * @param suspNodeData
	 * @param previousNodeCS
	 * @throws IOException
	 */
	private void redistributeDataSuspicious(int suspIndex, NodeData suspNodeData, ConfigCommInterface previousNodeCS) throws IOException {		
		ConfigCommInterface nextNodeCS =
				sortedConfigStores.get(suspIndex);
		ConfigCommInterface nextnextNodeCS =
				sortedConfigStores.get((suspIndex + 1) % sortedConfigStores.size());
		String newMetadata = metadataHandler.toString();

		try {
			if (!nextNodeCS.updateMetadata(newMetadata)) {
				logger.error("The metadata may not have been updated correctly on successor node " + nextNodeCS.getServerAddress());
			} else {
				logger.info("Metadata updated on successor node " + nextNodeCS.getServerAddress());
			}
		} catch (InterruptedException e) {
			logger.error("The metadata may not have been updated correctly on successor node " + nextNodeCS.getServerAddress());
		}

		if (!previousNodeCS.lockWrite()) {
			logger.error("The previous node may not have been write-locked correctly.");
		} else {
			logger.debug("Previous node write-locked.");
		}

		// COPY r2 range of SUSP from PHYSICAL PREVIOUS to PHYSICAL SUCCESSOR

		if (!previousNodeCS.copyData(nextNodeCS.getServerAddress(),
				suspNodeData.getMinR2HashKey(), suspNodeData.getMaxR2minR1HashKey())) {
			logger.error("The R2 data of suspicious node may not have been copied correctly from previous node " + previousNodeCS.getServerAddress() + " to next node.");
		} else {
			logger.info("R2 data of suspicious node copied from previous node " + previousNodeCS.getServerAddress() + " to next node.");
		}

		try {
			if (!nextnextNodeCS.updateMetadata(newMetadata)) {
				logger.error("The metadata may not have been updated correctly on after-successor node " + nextnextNodeCS.getServerAddress());
			} else {
				logger.info("Metadata updated on after-successor node " + nextnextNodeCS.getServerAddress());
			}

		} catch (InterruptedException e) {
			logger.error("The metadata may not have been updated correctly on after-successor node " + nextnextNodeCS.getServerAddress());
		}

		// COPY r1 range of SUSP from PHYSICAL PREVIOUS to  PHYSICAL AFTER-SUCCESSOR

		if (!previousNodeCS.copyData(nextnextNodeCS.getServerAddress(),
				suspNodeData.getMaxR2minR1HashKey(), suspNodeData.getMinWriteHashKey())) {
			logger.error("The R1 data of suspicious node may not have been copied correctly from previous node " + previousNodeCS.getServerAddress() + " to after-successor node.");
		} else {
			logger.info("R1 data of suspicious node copied from previous node " + previousNodeCS.getServerAddress() + " to after-successor node.");
		}
	}

	/**
	 * COPY r2 range of SUSP from PHYSICAL PREVIOUS to PHYSICAL SUCCESSOR
	 * COPY r1 range of SUSP from PHYSICAL PREVIOUS to PHYSICAL AFTER-SUCCESSOR
	 * @param suspIndex
	 * @param suspNodeData
	 * @param previousNodeCS
	 * @throws IOException
	 */
	private void redistributeDataSuspicious(int suspIndex, NodeData suspNodeData,  ConfigCommInterface beforePreviousNodeCS, ConfigCommInterface nextNodeCS) throws IOException {		
		ConfigCommInterface previousNodeCS =
				sortedConfigStores.get((suspIndex - 1) % sortedConfigStores.size());
		ConfigCommInterface nextnextNodeCS =
				sortedConfigStores.get((suspIndex + 1) % sortedConfigStores.size());
		String newMetadata = metadataHandler.toString();

		try {
			if (!nextNodeCS.updateMetadata(newMetadata)) {
				logger.error("The metadata may not have been updated correctly on successor node " + nextNodeCS.getServerAddress());
			} else {
				logger.info("Metadata updated on successor node " + nextNodeCS.getServerAddress());
			}
		} catch (InterruptedException e) {
			logger.error("The metadata may not have been updated correctly on successor node " + nextNodeCS.getServerAddress());
		}

		if (!beforePreviousNodeCS.lockWrite()) {
			logger.error("The before-previous node may not have been write-locked correctly.");
		} else {
			logger.debug("Before-previous node write-locked.");
		}

		// COPY r2 range of SUSP from PHYSICAL BEFORE-PREVIOUS to PHYSICAL SUCCESSOR

		if (!beforePreviousNodeCS.copyData(nextNodeCS.getServerAddress(),
				suspNodeData.getMinR2HashKey(), suspNodeData.getMaxR2minR1HashKey())) {
			logger.error("The R2 data of suspicious node may not have been copied correctly from previous node " + previousNodeCS.getServerAddress() + " to next node.");
		} else {
			logger.info("R2 data of suspicious node copied from previous node " + beforePreviousNodeCS.getServerAddress() + " to next node.");
		}

		try {
			if (!nextnextNodeCS.updateMetadata(newMetadata)) {
				logger.error("The metadata may not have been updated correctly on after-successor node " + nextnextNodeCS.getServerAddress());
			} else {
				logger.info("Metadata updated on after-successor node " + nextnextNodeCS.getServerAddress());
			}

		} catch (InterruptedException e) {
			logger.error("The metadata may not have been updated correctly on after-successor node " + nextnextNodeCS.getServerAddress());
		}

		if (!nextNodeCS.lockWrite()) {
			logger.error("The successor node may not have been write-locked correctly.");
		} else {
			logger.debug("Successor node write-locked.");
		}

		// COPY r1 range of SUSP from PHYSICAL SUCCESSOR to  PHYSICAL AFTER-SUCCESSOR

		if (!nextNodeCS.copyData(nextnextNodeCS.getServerAddress(),
				suspNodeData.getMaxR2minR1HashKey(), suspNodeData.getMinWriteHashKey())) {
			logger.error("The R1 data of suspicious node may not have been copied correctly from successor node " + nextNodeCS.getServerAddress() + " to after-successor node.");
		} else {
			logger.info("R1 data of suspicious node copied from successor node " + nextNodeCS.getServerAddress() + " to after-successor node.");
		}
	}



	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated with a server.");

		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost with a server.");
			System.out.print(PROMPT);
		}
	}

	@Override
	public void handleNewMessage(TextMessage msg) {
		AdminMessage message = AdminMessageImpl.unmarshal(msg);
		if (message.getStatus().equals(common.messages.AdminMessage.StatusType.SUSPICIOUS)) {
			try {
				handleSuspiciousNode(message.getKey(), Integer.valueOf(message.getValue()));
			} catch (NumberFormatException | NoSuchAlgorithmException
					| IOException e) {
				logger.error("Suspicion message could not be interpreted" , e);
			}
		}
	}


	/**
	 * Main entry point for the ECSClient application. 
	 * @param args contains the loglevel at args[0].
	 * @throws NoSuchAlgorithmException 
	 */
	public static void main(String[] args) {
		try {
			if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: ECS <loglevel> !");
			} else {
				new LogSetup("logs/ecs.log", Level.toLevel(args[0]));
				ECSInterface app = new ECSInterface();
				app.run();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

}