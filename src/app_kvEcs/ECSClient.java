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
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.cache_strategies.Strategy;
import common.messages.TextMessage;
import common.metadata.MetadataHandler;
import common.metadata.NodeData;
import client.KVSocketListener;

public class ECSClient implements KVSocketListener {

	private List<String[]> possibleRemainingNodes;
	private List<String> sortedNodeHashes;
	private List<ConfigStore> sortedConfigStores;

	private int numberOfUsedNodes = 0;
	private MetadataHandler metadataHandler;

	private static final Logger logger = Logger.getLogger(ECSClient.class);
	private final static String hashingAlgorithm = "MD5";
	private final static String PROMPT = "ECSClient> ";

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
//		String script = "script.sh";
<<<<<<< Updated upstream
//		String script = "ssh -n " + hostIp + " nohup java -jar C:/Users/Clotilde/git/BasicStorageServer/ms3-server.jar "
//					+ portNumber + " " + logLevel.toUpperCase() + " & ";
		String script = "java -jar C:/Users/Clotilde/git/BasicStorageServer/ms3-server.jar "
				+ port + " " + logLevel.toUpperCase();
=======
		String script = "ssh -n " + hostIp + " nohup java -jar /Users/nadiastraton/git/BasicStorageServer/ms3-server.jar "
					+ portNumber + " " + logLevel.toUpperCase() + " & ";
>>>>>>> Stashed changes
		Runtime run = Runtime.getRuntime();
		try {
			proc = run.exec(script);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected int initService(int numberOfNodes, int cacheSize, Strategy displacementStrategy)
			throws NoSuchAlgorithmException, IOException{
		if (numberOfNodes > possibleRemainingNodes.size()) {
			numberOfNodes = possibleRemainingNodes.size();
		}

		logger.debug("Initiating the service...");
		sortedConfigStores = new ArrayList<ConfigStore>();
		sortedNodeHashes = new ArrayList<>();

		for (int k = 0; k < numberOfNodes ; k++) {
			String[] line = possibleRemainingNodes.remove(0);
			addNodeToLists(line);
		}

		sortNodeLists();
		metadataHandler = new MetadataHandler(getMetadata(sortedNodeHashes));

		// TODO launch nodes via SSH with arg <port>

		String metadata = metadataHandler.toString();
		for (ConfigStore cs : sortedConfigStores) {
			cs.addListener(this);
			cs.initKVServer(metadata, cacheSize, displacementStrategy);
		}

		logger.debug("Possible nodes : \n" + printPossible());
		logger.debug("Used nodes : \n" + printNodes());

		numberOfUsedNodes = numberOfNodes;
		return numberOfUsedNodes;
	}

	private java.util.List<NodeData> getMetadata(java.util.List<String> sortedList) {
		LinkedList<NodeData> list = new LinkedList<>();
		int size = sortedList.size();

		for (int i=0; i<size; i+=4) {
			String minHashKey = sortedList.get((i-1 + size) % size);
			list.add(new NodeData(sortedList.get(i), sortedList.get(i+1),
					Integer.valueOf(sortedList.get(i+2)), minHashKey, sortedList.get(i+3)));
		}
		return list;
	}

	protected void start(){
		for (ConfigStore cs : sortedConfigStores) {
			if (!cs.startServer()) {
				logger.error("Some server may not have been started correctly.");
			}
		}
	}

	protected void stop(){
		if (sortedConfigStores == null) {
			return;
		}
		for (ConfigStore cs : sortedConfigStores) {
			if (!cs.stopServer()) {
				logger.error("Some server may not have been stopped correctly.");
			}
		}

	}

	protected void shutdown(){
		if (sortedConfigStores == null) {
			return;
		}
		stop();
		for (ConfigStore cs : sortedConfigStores) {
			if (!cs.shutdown()) {
				logger.error("Some server may not have been shut down correctly.");
			}
			cs = null;
		}
		sortedConfigStores = null;
		sortedNodeHashes = null;
		numberOfUsedNodes = 0;
	}

	protected int addNode(int cacheSize, Strategy displacementStrategy)
			throws NoSuchElementException, NoSuchAlgorithmException{
		int size = possibleRemainingNodes.size(); 
		if (size < 1) {
			throw new NoSuchElementException("There is no more node to add.");
		}

		int random = (int) (Math.random() * (size - 1));
		String[] params = possibleRemainingNodes.remove(random);
		logger.debug("Adding node " + params[0]);
		try {
			addNodeToLists(params);
		} catch (IllegalArgumentException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String nodeName = params[0];

		sortNodeLists();
		metadataHandler = new MetadataHandler(getMetadata(sortedNodeHashes));

		// TODO launch node via SSH with arg <port>

		int index = findCsIndex(nodeName);
		ConfigStore newNodeCS = sortedConfigStores.get(index);
		newNodeCS.addListener(this);

		String updatedMetadata = metadataHandler.toString();
		if (!newNodeCS.initKVServer(updatedMetadata, cacheSize, displacementStrategy)) {
			logger.error("The node may not have been initialized correctly.");
		} else {
			logger.info("New node initialized.");
		}
		if (!newNodeCS.startServer()) {
			logger.error("The node may not have been started correctly.");
		} else {
			logger.info("New node started.");
		}

		ConfigStore nextNodeCS = sortedConfigStores.get((index + 1) % sortedConfigStores.size());
		String hashOfNewNode = sortedNodeHashes.get(index*4 +3);
		String[] newNodeAddress = new String[] {params[1], params[2]};


		if (!nextNodeCS.lockWrite()) {
			logger.error("The successor node may not have been write-locked correctly.");
		} else {
			logger.info("Successor node write-locked.");
		}
		if (!nextNodeCS.moveData(hashOfNewNode, newNodeAddress)) {
			logger.error("The data may not have been moved correctly.");
		} else {
			logger.info("Data moved to new node.");
		}

		for (ConfigStore c : sortedConfigStores) {
			try {
				if (!c.updateMetadata(updatedMetadata)) {
					logger.error("The metadata may not have been updated correctly on a given server.");
				}
			} catch (InterruptedException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		logger.info("Metadata updated for all nodes");

		if (!nextNodeCS.unlockWrite()) {
			logger.error("The successor node may not have been write-unlocked correctly.");
		} else {
			logger.info("Successor node write-unlocked.");
		}

		logger.debug("Possible nodes : \n" + printPossible());
		logger.debug("Used nodes : \n" + printNodes());

		return ++numberOfUsedNodes;
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

	protected int removeNode() throws IOException{
		if (numberOfUsedNodes <= 1) {
			return 0;
		}

		int random = (int) (Math.random() * (sortedConfigStores.size() - 1));
		String toRemoveName = sortedNodeHashes.get(random*4);
		logger.info("Removing node " + toRemoveName);
		int toRemoveIndex = findCsIndex(toRemoveName);

		String toRemoveHash = sortedNodeHashes.get(toRemoveIndex*4 + 3);
		ConfigStore removed = removeNodeFromLists(toRemoveName);
		metadataHandler = new MetadataHandler(getMetadata(sortedNodeHashes));

		removed.lockWrite();
		System.out.println("successor node is " + sortedNodeHashes.get(toRemoveIndex*4 % sortedNodeHashes.size()));
		ConfigStore successor = sortedConfigStores.get(toRemoveIndex % sortedConfigStores.size());
		String newMetadata = metadataHandler.toString();
		try {
			if (!successor.updateMetadata(newMetadata)) {
				logger.error("The metadata may not have been updated correctly on successor node.");
			} else {
				logger.info("Metadata updated on successor node.");
			}

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (!removed.moveData(toRemoveHash, successor.getIpAndPort())) {
			logger.error("The data may not have been moved correctly to successor node.");
		} else {
			logger.info("Data moved to successor node.");
		}


		for (ConfigStore cs : sortedConfigStores) {
			try {
				if (!cs.updateMetadata(newMetadata)) {
					logger.error("Some server may not have updated its metadata correctly.");
				}

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (!removed.shutdown()) {
			logger.error("The removed node may not have been shut down correctly.");
		}

		logger.debug("Possible nodes : \n" + printPossible());
		logger.debug("Used nodes : \n" + printNodes());

		return --numberOfUsedNodes;
	}


	private ConfigStore removeNodeFromLists(String nodeName) {
		int size = sortedNodeHashes.size();		
		int index = 0;
		for(int i = 0; i < size; i+=4){
			if(nodeName.equals(sortedNodeHashes.get(i))){
				index = i;
				sortedNodeHashes.remove(i); // remove name
				String ip = sortedNodeHashes.remove(i); // remove ip
				String port = sortedNodeHashes.remove(i); // remove port
				sortedNodeHashes.remove(i); // remove hash

				possibleRemainingNodes.add(new String[] {nodeName, ip, port});
				break;
			}
		}
		return sortedConfigStores.remove(index/4);
	}

	private void addNodeToLists(String[] nodeData)
			throws NoSuchAlgorithmException, IllegalArgumentException, IOException {
		launchSSH(nodeData[1], nodeData[2], "DEBUG");
		
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
	public void handleNewMessage(TextMessage msg) {}

	/**
	 * Main entry point for the ECSClient application. 
	 * @param args contains the loglevel at args[0].
	 * @throws NoSuchAlgorithmException 
	 */
	public static void main(String[] args) {
		try {
			if(args.length != 0) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: ECS <loglevel> !");
			} else {
				new LogSetup("logs/ecs.log", Level.toLevel(1));
				ECSInterface app = new ECSInterface();
				app.run();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
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

}
