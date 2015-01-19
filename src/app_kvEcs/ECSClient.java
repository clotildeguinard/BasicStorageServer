package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

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

public class ECSClient implements SocketListener {
	private static final Logger logger = Logger.getLogger(ECSClient.class);
	private final static String hashingAlgorithm = "MD5";
	private final static String PROMPT = "ECSClient> ";

	private boolean isHandlingSuspiciousNode = false;
	private HashMap<Address, Long> suspicionMap = new HashMap<>();

	private List<String[]> possibleRemainingNodes;
	private List<String> sortedNodeHashes;
	private List<ConfigCommInterface> sortedConfigStores;
	private int defaultCacheSize;
	private Strategy defaultDisplacementStrategy;
	private boolean nodesStarted = false;

	private int nbUsedNodes = 0;
	private MetadataHandler metadataHandler;

	public ECSClient(String configLocation) {
		this.possibleRemainingNodes = new ArrayList<String[]>();
		InputStream fis;
		BufferedReader br = null;
		try{
			fis = new FileInputStream(configLocation);
			br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
			String line;
			while ((line = br.readLine()) != null) {
				possibleRemainingNodes.add(line.split(" "));
			}

		} catch (IOException e) {
			System.out.println("An error occurred when starting the ECS client"
					+ "- Application terminated.");
			logger.fatal("An error occurred when trying to read from config file"
					+ "- Application terminated.", e);
			System.exit(1);
		} finally {
			try {
				if (br != null) {
					br.close();		
					br = null;
				}
				fis = null;
			} catch (IOException e1) {
				logger.error("Unable to close io!");
			}
		}
	}

	private void launchSSH(String hostIp, String port, String logLevel) {
		Process proc;

		//		String script = "ssh -n " + hostIp + " nohup java -jar C:/Users/Clotilde/git/BasicStorageServer/ms3-server.jar "
		//					+ portNumber + " " + logLevel.toUpperCase() + " & ";



		//
		//		String script = "java -jar /Users/nadiastraton/git/BasicStorageServer/ms3-server.jar "
		//
		//				+ port + " " + logLevel.toUpperCase();


		String currentPath = ClassLoader.getSystemResource("").toString();
		System.out.println(currentPath);
		String serverJarPath = currentPath.replace("/bin/", "/").substring(6);

		String script = "java -jar " + serverJarPath + "ms3-server.jar "
				+ port + " " + logLevel.toUpperCase();

		System.out.println(script);

		//		script = "java -jar C:/Users/Clotilde/git/BasicStorageServer/ms3-server.jar "
		//				+ port + " " + logLevel.toUpperCase();


		Runtime run = Runtime.getRuntime();
		try {
			proc = run.exec(script);
		} catch (IOException e) {
			e.printStackTrace();
		}
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
			throws NoSuchAlgorithmException, IOException{
		this.defaultCacheSize = cacheSize;
		this.defaultDisplacementStrategy = displacementStrategy;

		if (numberOfNodes > possibleRemainingNodes.size()) {
			numberOfNodes = possibleRemainingNodes.size();
		}

		logger.debug("Initiating the service...");
		sortedConfigStores = new ArrayList<ConfigCommInterface>();
		sortedNodeHashes = new ArrayList<>();

		for (int k = 0; k < numberOfNodes ; k++) {
			String[] line = possibleRemainingNodes.remove(0);
			addNodeToLists(line);
		}

		sortNodeLists();
		metadataHandler = new MetadataHandler(getMetadata(sortedNodeHashes));

		// TODO launch nodes via SSH with arg <port>

		String metadata = metadataHandler.toString();
		for (ConfigCommInterface cs : sortedConfigStores) {
			((ConfigStore) cs).addListener(this);
			cs.initKVServer(metadata, cacheSize, displacementStrategy);
		}

		logger.debug("Possible nodes : \n" + printPossible());
		logger.debug("Used nodes : \n" + printNodes());

		startHeartbeats();
		nbUsedNodes = numberOfNodes;
		return nbUsedNodes;
	}

	private java.util.List<NodeData> getMetadata(java.util.List<String> sortedList) {
		LinkedList<NodeData> list = new LinkedList<>();
		int size = sortedList.size();

		for (int i=0; i<size; i+=4) {
			String minWriteHashKey = sortedList.get((i-1 + size) % size);
			String maxR2minR1HashKey = sortedList.get((i-5 + 2 * size) % size);
			String minR2HashKey = sortedList.get((i-9 + 3 * size) % size);
			list.add(new NodeData(sortedList.get(i), sortedList.get(i+1),
					Integer.valueOf(sortedList.get(i+2)), sortedList.get(i+3), minWriteHashKey, maxR2minR1HashKey, minR2HashKey));
		}
		return list;
	}

	/**
	 * Start all nodes participating in the service
	 * i.e. disable answer to queries from clients
	 */
	protected void start(){
		logger.info("Starting all used nodes.");
		for (ConfigCommInterface cs : sortedConfigStores) {
			if (!cs.startServer()) {
				logger.error("Server " + cs.getServerAddress() + " may not have been"
						+ " started correctly.");
			}
		}
		nodesStarted = true;
	}

	/**
	 * Stop all nodes participating in the service
	 * i.e. disable answer to queries from clients
	 */
	protected void stop(){
		if (sortedConfigStores == null) {
			return;
		}
		logger.info("Stopping all used nodes.");
		for (ConfigCommInterface cs : sortedConfigStores) {
			if (!cs.stopServer()) {
				logger.error("Server " + cs.getServerAddress() + " may not have been"
						+ " stopped correctly.");
			}
		}
		nodesStarted = false;
	}

	/**
	 * Shutdown all nodes participating in the service
	 * i.e. stop them, stop heartbeat and close connection to them
	 */
	protected void shutdown(){
		if (sortedConfigStores == null) {
			return;
		}
		logger.info("Shutting down all used nodes.");
		for (ConfigCommInterface cs : sortedConfigStores) {
			if (!cs.shutdown()) {
				logger.error("Server " + cs.getServerAddress() + " may not have been"
						+ " shut down correctly.");
			}
			cs = null;
		}
		sortedConfigStores = null;
		sortedNodeHashes = null;
		nbUsedNodes = 0;
		nodesStarted = false;
	}

	protected void stopHeartbeats(){
		logger.info("Stopping heartbeat on all used nodes");
		for (ConfigCommInterface cs : sortedConfigStores) {
			if (!cs.stopHeartbeat()) {
				logger.error("Server " + cs.getServerAddress() + " may not have been"
						+ " stopped to heartbeat correctly.");
			}
		}
	}

	protected void startHeartbeats(){
		logger.info("Starting heartbeat on all used nodes");
		for (ConfigCommInterface cs : sortedConfigStores) {
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
		for (ConfigCommInterface c : sortedConfigStores) {
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
	protected int addNode(int cacheSize, Strategy displacementStrategy)
			throws NoSuchElementException, NoSuchAlgorithmException, IOException{

		int size = possibleRemainingNodes.size(); 
		if (size < 1) {
			throw new NoSuchElementException("There is no more node to add.");
		}

		stopHeartbeats();

		try {
			int random = (int) (Math.random() * (size - 1));
			String[] newNodeParams = possibleRemainingNodes.remove(random);
			String nodeName = newNodeParams[0];
			logger.debug("Adding node " + nodeName);
			
			try {
				addNodeToLists(newNodeParams);
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
			metadataHandler = new MetadataHandler(getMetadata(sortedNodeHashes));

			// TODO launch node via SSH with arg <port>

			int index = findCsIndex(nodeName);
			ConfigCommInterface newNodeCS = sortedConfigStores.get(index);
			((ConfigStore) newNodeCS).addListener(this);

			String updatedMetadata = metadataHandler.toString();
			if (!newNodeCS.initKVServer(updatedMetadata, cacheSize, displacementStrategy)) {
				logger.error("The new node may not have been initialized correctly.");
			} else {
				logger.info("New node initialized.");
			}
			if (nodesStarted) {
				if (!newNodeCS.startServer()) {
					logger.error("The new node may not have been started correctly.");
				} else {
					logger.info("New node started.");
				}
			}

			ConfigCommInterface nextNodeCS = sortedConfigStores.get((index + 1) % sortedConfigStores.size());
			ConfigCommInterface nextnextNodeCS = sortedConfigStores.get((index + 2) % sortedConfigStores.size());

			redistributeDataAdd(newNodeCS.getServerAddress(), nextNodeCS, nextnextNodeCS);

			updateAllMetadata();

			if (!nextNodeCS.unlockWrite()) {
				logger.error("The successor node may not have been write-unlocked correctly.");
			} else {
				logger.info("Successor node write-unlocked.");
			}

			logger.debug("Possible nodes : \n" + printPossible());
			logger.debug("Used nodes : \n" + printNodes());

			return ++nbUsedNodes; 

		} finally {
			startHeartbeats();
		}
	}

	private void redistributeDataAdd(Address newNodeAddress, ConfigCommInterface nextNodeCS, ConfigCommInterface nextnextNodeCS) {

		NodeData newNodeData = metadataHandler.getNodeData(newNodeAddress.getIp(), newNodeAddress.getPort());

		if (!nextNodeCS.lockWrite()) {
			logger.error("The successor node may not have been write-locked correctly.");
		} else {
			logger.info("Successor node write-locked.");
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
	protected int removeNode() throws IOException{
		if (nbUsedNodes <= 1) {
			return 0;
		}

		try {

			int toRemoveIndex = (int) (Math.random() * (sortedConfigStores.size() - 1));

			String toRemoveIp = sortedNodeHashes.get(toRemoveIndex * 4 + 1);
			int toRemovePort = Integer.valueOf(sortedNodeHashes.get(toRemoveIndex * 4 + 2));
			logger.info("Removing node " + toRemoveIp + ":" + toRemovePort);

			NodeData removedNodeData = metadataHandler.getNodeData(toRemoveIp, toRemovePort);
			ConfigCommInterface removedNodeCS = removeNodeFromLists(toRemoveIndex);
			metadataHandler = new MetadataHandler(getMetadata(sortedNodeHashes));

			stopHeartbeats();

			removedNodeCS.lockWrite();

			redistributeDataRemove(toRemoveIndex, removedNodeData, removedNodeCS);

			updateAllMetadata();

			if (!removedNodeCS.shutdown()) {
				logger.error("The removed node may not have been shut down correctly.");
			}

			logger.debug("Possible nodes : \n" + printPossible());
			logger.debug("Used nodes : \n" + printNodes());

			return --nbUsedNodes;

		} finally {
			startHeartbeats();
		}
	}

	private void redistributeDataRemove(int toRemoveIndex, NodeData removedNodeData, ConfigCommInterface toRemoveNodeCS) throws IOException {		
		ConfigCommInterface nextNodeCS = sortedConfigStores.get(toRemoveIndex % sortedConfigStores.size());
		ConfigCommInterface nextnextNodeCS = sortedConfigStores.get((toRemoveIndex + 1) % sortedConfigStores.size());
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

		// COPY r2 range from TOREMOVE to NEXT

		if (!toRemoveNodeCS.copyData(nextNodeCS.getServerAddress(),
				removedNodeData.getMinR2HashKey(), removedNodeData.getMaxR2minR1HashKey())) {
			logger.error("The R2 data may not have been copied correctly from removed node " + toRemoveNodeCS.getServerAddress() + " to successor node.");
		} else {
			logger.info("R2 data copied from removed node " + toRemoveNodeCS.getServerAddress() + " to successor node.");
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

		// COPY r1 range from TOREMOVE to NEXT-NEXT
		if (!toRemoveNodeCS.copyData(nextnextNodeCS.getServerAddress(),
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

		if (isHandlingSuspiciousNode) {
			return;

		} else {
			Address a = new Address(ip, port);
			long time = System.currentTimeMillis();
			synchronized(this) {
				if (suspicionMap.containsKey(a) && (time - suspicionMap.get(a) < 7450) ) {
					logger.info("Has to handle suspicious node " + a);
					isHandlingSuspiciousNode = true;
					handleSuspiciousNodeBis(ip, port);
					suspicionMap.remove(a);
					isHandlingSuspiciousNode = false;
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
	protected void handleSuspiciousNodeBis(String ip, int port)
			throws NoSuchAlgorithmException, IOException {

		int suspIndex = -1;
		String suspName = null;
		for (int i = 0; i < sortedNodeHashes.size(); i+=4){
			if (sortedNodeHashes.get(i+1).equals(ip) && sortedNodeHashes.get(i+2).equals(Integer.toString(port))){
				suspIndex = i / 4;
				suspName = sortedNodeHashes.get(i);
			}
		}
		logger.info("Suspicious node " + suspName);

		NodeData suspNodeData = metadataHandler.getNodeData(ip, port);
		ConfigCommInterface suspNodeCS = removeNodeFromLists(suspIndex);

		stopHeartbeats();

		try {
			metadataHandler = new MetadataHandler(getMetadata(sortedNodeHashes));

			suspNodeCS.lockWrite();

			redistributeDataSuspicious(suspIndex, suspNodeData,
					sortedConfigStores.get((suspIndex - 1 + sortedConfigStores.size()) % sortedConfigStores.size()));

			updateAllMetadata();

			if (!suspNodeCS.shutdown()) {
				logger.error("The removed node may not have been shut down correctly.");
			}

			logger.debug("Possible nodes : \n" + printPossible());
			logger.debug("Used nodes : \n" + printNodes());

			addNode(defaultCacheSize, defaultDisplacementStrategy);
		} finally {
			startHeartbeats();
		}
	}

	private void redistributeDataSuspicious(int suspIndex, NodeData suspNodeData, ConfigCommInterface previousNodeCS) throws IOException {		
		ConfigCommInterface nextNodeCS =
				sortedConfigStores.get(suspIndex % sortedConfigStores.size());
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

		// COPY r2 range of SUSP from PREVIOUS to NEXT

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

		// COPY r1 range of SUSP from PREVIOUS to NEXT-NEXT
		if (!previousNodeCS.copyData(nextnextNodeCS.getServerAddress(),
				suspNodeData.getMaxR2minR1HashKey(), suspNodeData.getMinWriteHashKey())) {
			logger.error("The R1 data of suspicious node may not have been copied correctly from previous node " + previousNodeCS.getServerAddress() + " to after-successor node.");
		} else {
			logger.info("R1 data of suspicious node copied from previous node " + previousNodeCS.getServerAddress() + " to after-successor node.");
		}
	}

	private int findCsIndex(String nodeName) {
		int max = sortedNodeHashes.size();
		for (int i=0; i<max; i += 4) {
			if (sortedNodeHashes.get(i).equals(nodeName)) {
				return i/4;
			}
		}
		return -1;
	}


	private ConfigCommInterface removeNodeFromLists(int index) {

		String name = sortedNodeHashes.remove(index * 4); // remove name
		String ip = sortedNodeHashes.remove(index * 4); // remove ip
		String port = sortedNodeHashes.remove(index * 4); // remove port
		sortedNodeHashes.remove(index * 4); // remove hash

		possibleRemainingNodes.add(new String[] {name, ip, port});

		return sortedConfigStores.remove(index);
	}

	private void addNodeToLists(String[] nodeData)
			throws NoSuchAlgorithmException, IllegalArgumentException, IOException {
		//		launchSSH(nodeData[1], nodeData[2], "DEBUG");

		sortedConfigStores.add(new ConfigStore(nodeData[1], Integer.parseInt(nodeData[2])));

		String IpAndPort = new StringBuilder(nodeData[1]).append(";").append(nodeData[2]).toString();

		String hashedKey = new BigInteger(1,MessageDigest.getInstance(hashingAlgorithm).
				digest(IpAndPort.getBytes("UTF-8"))).toString(16);

		sortedNodeHashes.add(nodeData[0]);
		sortedNodeHashes.add(nodeData[1]);
		sortedNodeHashes.add(nodeData[2]);
		sortedNodeHashes.add(hashedKey);

	}

	private void sortNodeLists(){
		logger.debug("Sorting the nodes...");
		int size = sortedNodeHashes.size();
		if (size <= 4) {
			logger.debug("No sorting: only one node.");
			return;
		}
		String current;

		for (int i = 0; i < size ; i+=4){
			String max = sortedNodeHashes.get(0);
			int indexOfMax = 0;
			for (int j = 3; j < size-i; j+=4){
				current = sortedNodeHashes.get(i);
				if(current.compareTo(max) >= 0){
					max = current;
					indexOfMax = j;
				}
			}
			Collections.swap(sortedNodeHashes, indexOfMax, size-1-i);
			Collections.swap(sortedNodeHashes, indexOfMax-1, size-1-(i+1));
			Collections.swap(sortedNodeHashes, indexOfMax-2, size-1-(i+2));
			Collections.swap(sortedNodeHashes, indexOfMax-3, size-1-(i+3));

			Collections.swap(sortedConfigStores, indexOfMax/4, (size-1-i)/4);
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


	private String printNodes() {
		String s = "";
		if (sortedNodeHashes == null) {
			return s;
		}
		int j = 0;
		for (String i : sortedNodeHashes) {
			j++;
			s = s + i ;
			if (j%4 == 0) {
				s+= "\n";
			} else {
				s+= ";";
			}
		}
		return s;
	}

	private String printPossible() {
		String s = "";
		if (possibleRemainingNodes == null) {
			return s;
		}
		for (String[] i : possibleRemainingNodes) {
			for (String k : i){
				s = s + k + ";";
			}
			s+= "\n";
		}
		return s;
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