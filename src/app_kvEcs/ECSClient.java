package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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
import client.KVAdminCommModule;
import client.KVSocketListener;
import client.KVSocketListener.SocketStatus;

public class ECSClient implements KVSocketListener {

	protected InputStream    fis;
	protected BufferedReader br;
	protected String         line;
	private List<String> sortedNodeHashes = new ArrayList<String>();
	private List<ConfigStore> sortedConfigStores = new ArrayList<ConfigStore>();

	private int numberOfUsedNodes = 0;
	private MetadataHandler metadataHandler;
	private Logger logger = Logger.getLogger(getClass().getSimpleName());
	private final static String configLocation = "./src/app_kvEcs/ecs.config.txt";

	protected void initService(int numberOfNodes, int cacheSize, Strategy displacementStrategy)
			throws NoSuchAlgorithmException{
		logger.debug("Initiating the service...");
		
		try {
			fis = new FileInputStream(configLocation);
		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
		int added = 0;

		while ((line = br.readLine()) != null && numberOfNodes > added) {
			logger.info("Adding node " + line);
			addNodeToLists(line.split(" "));
			added ++;		
		}
		br.close();		
		br = null;
		fis = null;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sortNodeLists();
		metadataHandler = new MetadataHandler(getMetadata(sortedNodeHashes));

		// TODO launch nodes via SSH with arg <port>

		String metadata = metadataHandler.toString();
		for (ConfigStore cs : sortedConfigStores) {
			cs.addListener(this);
			cs.initKVServer(metadata, cacheSize, displacementStrategy);
		}

		numberOfUsedNodes = numberOfNodes;
	}

	private java.util.List<NodeData> getMetadata(java.util.List<String> sortedList) {
		LinkedList<NodeData> list = new LinkedList<>();
		int size = sortedList.size();
		for (int i=0; i<size; i+=4) {
			String minHashKey = sortedList.get((i-1 + size) % size);
			list.add(new NodeData(sortedList.get(i), sortedList.get(i+1), Integer.valueOf(sortedList.get(i+2)), minHashKey, sortedList.get(i+3)));
		}
		return list;
	}

	protected void start(){
		for (ConfigStore cs : sortedConfigStores) {
			cs.startServer();
		}
	}

	protected void stop(){
		for (ConfigStore cs : sortedConfigStores) {
			cs.stopServer();
		}
	}

	protected void shutdown(){
		stop();
		for (ConfigStore cs : sortedConfigStores) {
			cs.shutdown();
		}
	}

	protected void addNode(int cacheSize, Strategy displacementStrategy) throws IOException, NoSuchElementException, NoSuchAlgorithmException{

		fis = new FileInputStream(configLocation);
		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));

		for(int i = 0; i <= numberOfUsedNodes; ++i) {
			line = br.readLine();
		}		
		br.close();		
		br = null;
		fis = null;

		if (line == null) {
			throw new NoSuchElementException("There is no more node to add.");
		}
		logger.debug("Adding node " + line);
		String[] params = line.split(" ");
		addNodeToLists(params);
		String nodeName = params[0];

		sortNodeLists();
		metadataHandler = new MetadataHandler(getMetadata(sortedNodeHashes));

		// TODO launch node via SSH with arg <port>

		// TODO connect via socket to this node
		int index = findCsIndex(nodeName);
		ConfigStore newNodeCS = sortedConfigStores.get(index);
		newNodeCS.addListener(this);

		String updatedMetadata = metadataHandler.toString();
		newNodeCS.initKVServer(updatedMetadata, cacheSize, displacementStrategy);
		newNodeCS.startServer();
		
		ConfigStore nextNodeCS = sortedConfigStores.get((index + 1) % sortedConfigStores.size());
		String hashOfNewNode = sortedNodeHashes.get(index*4 +3);
		String[] newNodeAddress = new String[] {params[1], params[2]};
		
		nextNodeCS.lockWrite();
		nextNodeCS.moveData(hashOfNewNode, newNodeAddress);
		
		for (ConfigStore c : sortedConfigStores) {
			try {
				c.updateMetadata(updatedMetadata);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		nextNodeCS.unlockWrite();
		// TODO remove stale data on nextNode????
		numberOfUsedNodes++;
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

	protected void removeNode() throws IOException{
		if (numberOfUsedNodes == 0) {
			throw new NoSuchElementException("There is no more node to remove.");
		}
		fis = new FileInputStream(configLocation);

		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
		for(int i = 0; i < numberOfUsedNodes; ++i) {
			line = br.readLine();
		}		
		br.close();		
		br = null;
		fis = null;

		// TODO invoke movedata on relevant node
		// TODO update metadata of all remaining nodes
		// TODO shutdown node

		removeNodeFromLists(line.split(" ")[0]);
		metadataHandler = new MetadataHandler(getMetadata(sortedNodeHashes));
		numberOfUsedNodes--;
	}


	private void removeNodeFromLists(String nodeName) {
		int size = sortedNodeHashes.size();		
		List<String> removedList = new ArrayList<String>();
		int index = 0;
		for(int i = 0; i < size; i++){
			if(nodeName.equals(sortedNodeHashes.get(i))){
				index = i;
				removedList.add(sortedNodeHashes.get(i));
				removedList.add(sortedNodeHashes.get(i+1));
				removedList.add(sortedNodeHashes.get(i+2));
				removedList.add(sortedNodeHashes.get(i+3));
				break;
			}
		}		
		sortedNodeHashes.removeAll(removedList);
		sortedConfigStores.remove(index/4);
	}

	private void addNodeToLists(String[] nodeData) throws NoSuchAlgorithmException, IllegalArgumentException, IOException {
		sortedConfigStores.add(new ConfigStore(nodeData[1], Integer.parseInt(nodeData[2])));

		String IpAndPort = new StringBuilder(nodeData[1]).append(";").append(nodeData[2]).toString();

		//combine ip and port and convert to hash.
		String hashedKey = new BigInteger(1,MessageDigest.getInstance("MD5").digest(IpAndPort.getBytes("UTF-8"))).toString(16);
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
			String max = sortedNodeHashes.get(1);
			int indexOfMax = 0;
			for (int j = 3; j < size-i; j+=4){
				current = sortedNodeHashes.get(i);
				if(current.compareTo(max) >= 0){
					max = current;
					indexOfMax = j;
				}
			}
			Collections.swap(sortedNodeHashes, indexOfMax, size-i);
			Collections.swap(sortedNodeHashes, indexOfMax-1, size-(i+1));
			Collections.swap(sortedNodeHashes, indexOfMax-2, size-(i+2));
			Collections.swap(sortedNodeHashes, indexOfMax-3, size-(i+3));

			Collections.swap(sortedConfigStores, indexOfMax/4, (size-i)/4);
		}				
	}

    
    @Override
    public void handleStatus(SocketStatus status) {
        
    }
    
    @Override
    public void handleNewMessage(TextMessage msg) {
        System.out.println("Got new message in ECS");
        
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
