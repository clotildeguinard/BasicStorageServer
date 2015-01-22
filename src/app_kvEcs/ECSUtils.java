package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import common.metadata.Address;
import common.metadata.NodeData;

public class ECSUtils {
	protected static List<String[]> possibleRemainingNodes;
	protected static List<String> sortedNodeHashes;
	protected static List<ConfigCommInterface> sortedConfigStores;
	protected static List<ConfigCommInterface> uniqueConfigStores;

	private final static Logger logger = Logger.getLogger(ECSUtils.class);
	private final static String hashingAlgorithm = "MD5";
	private final static String serverLogLevel = "DEBUG";
	private final static int nbVirtualNodes = 4;
	private static int seed = 0;

	protected ECSUtils() {}

	protected void readConfigFile(String configLocation) throws IOException {
		possibleRemainingNodes = new ArrayList<String[]>();
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
			throw(e);
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

	protected void launchSSH(String ip, String port) throws URISyntaxException {

//		String sshScript = "ssh -n " + ip + " nohup java -jar C:/Users/Clotilde/git/BasicStorageServer/ms3-server.jar "
//				+ port + " " + serverLogLevel + " & ";
//
//
//		String currentPath = getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath().toString();
//
//		String serverJarPath = currentPath.substring(1, 42) + "ms3-server.jar";
//		String script = "java -jar " + serverJarPath + " " + port + " " + serverLogLevel;
//		System.out.println(script);
//
//		//		script = "java -jar " + "C:/Users/Clotilde/git/BasicStorageServer/ms3-server.jar" 
//		//				+ " " + port + " " + serverLogLevel;
//		//		String script = "java -jar /Users/nadiastraton/git/BasicStorageServer/ms3-server.jar"
//		//		+ " " + port + " " + serverLogLevel;
//
//		try {
//			Runtime run = Runtime.getRuntime();
//			run.exec(script);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}

	protected void mergeAdjacentVirtualNodes() {
		int i = 0;

		while (i < sortedConfigStores.size()) {
			ConfigCommInterface cs = sortedConfigStores.get(i);
			int iNext = (i + 1) % sortedConfigStores.size();

			if (cs.equals(sortedConfigStores.get(iNext))) {
				sortedConfigStores.remove(i);

				int j = Math.min(i, iNext) * 4;
				sortedNodeHashes.remove(j);
				sortedNodeHashes.remove(j);
				sortedNodeHashes.remove(j);
				sortedNodeHashes.remove(j);
			} else {
				i ++;
			}
		}
	}

	protected void addNodeToLists(String name, String ip, int port)
			throws NoSuchAlgorithmException, IllegalArgumentException, IOException {
		for (String[] params : possibleRemainingNodes) {
			if (params[0].equals(name)) {
				possibleRemainingNodes.remove(params);
				break;
			}
		}

		ConfigStore cs = new ConfigStore(ip, port);
		uniqueConfigStores.add(cs);

		for (int i = 0; i < nbVirtualNodes; i++) {
			sortedConfigStores.add(cs);

			String IpAndPort = new StringBuilder(ip).append(";").append(port)
					.append(";").append(seed + i).toString();

			String hashedKey = new BigInteger(1,MessageDigest.getInstance(hashingAlgorithm).
					digest(IpAndPort.getBytes("UTF-8"))).toString(16);

			sortedNodeHashes.add(name);
			sortedNodeHashes.add(ip);
			sortedNodeHashes.add(Integer.toString(port));
			sortedNodeHashes.add(hashedKey);
		}
		seed += nbVirtualNodes;

	}

	protected void sortNodeLists(){
		logger.debug("Sorting the nodes...");
		int size = sortedNodeHashes.size();
		if (size <= 4) {
			// no sorting : only one node
			return;
		}
		String current;

		for (int i = 0; i < size ; i+=4){
			String max = sortedNodeHashes.get(3);
			int indexOfMax = 3;

			for (int j = 3; j < size-i; j+=4){
				current = sortedNodeHashes.get(j);
				if(current.compareTo(max) >= 0){
					max = current;
					indexOfMax = (j - 3 + size) % size;
				}
			}

			Collections.swap(sortedNodeHashes, indexOfMax, size-4-i);
			Collections.swap(sortedNodeHashes, indexOfMax+1, size-3-i);
			Collections.swap(sortedNodeHashes, indexOfMax+2, size-2-i);
			Collections.swap(sortedNodeHashes, indexOfMax+3, size-1-i);

			Collections.swap(sortedConfigStores, indexOfMax/4, (size-4-i)/4);
		}
	}

	protected java.util.List<NodeData> getMetadataFromSortedList() {
		LinkedList<NodeData> list = new LinkedList<>();
		int size = sortedNodeHashes.size();

		for (int i=0; i<size; i+=4) {
			String minWriteHashKey = sortedNodeHashes.get((i-1 + size) % size);
			String maxR2minR1HashKey = sortedNodeHashes.get((i-5 + 2 * size) % size);
			String minR2HashKey = sortedNodeHashes.get((i-9 + 3 * size) % size);
			list.add(new NodeData(sortedNodeHashes.get(i), sortedNodeHashes.get(i+1),
					Integer.valueOf(sortedNodeHashes.get(i+2)), sortedNodeHashes.get(i+3), minWriteHashKey, maxR2minR1HashKey, minR2HashKey));
		}
		return list;
	}

	protected List<Integer> findVirtualNodeIndexes(Address a) {
		List<Integer> l = new ArrayList<Integer>();
		for (int i = 0; i < sortedConfigStores.size(); i ++) {
			if (sortedConfigStores.get(i).getServerAddress().isSameAddress(a)) {
				l.add(i);
			}
		}
		return l;
	}

	/**
	 * Remove all virtual nodes corresponding to given server from used-virtualnodes-lists
	 * @param toRemoveAddress
	 * @return list of removed virtual nodes indexes
	 */
	protected List<Integer> removeNodeFromLists(Address toRemoveAddress) {
		List<Integer> indexesTmp = new ArrayList<>();

		int k = 0;
		while (k < sortedConfigStores.size()) {
			ConfigCommInterface c = sortedConfigStores.get(k);
			if (toRemoveAddress.equals(c.getServerAddress())) {
				sortedConfigStores.remove(k);
				indexesTmp.add(k);
			} else {
				k++;
			}
		}

		String name = null;

		for (int i : indexesTmp) {
			int j = i * 4;
			name = sortedNodeHashes.remove(j); // remove name
			sortedNodeHashes.remove(j); // remove ip
			sortedNodeHashes.remove(j); // remove port
			sortedNodeHashes.remove(j); // remove hash
		}

		possibleRemainingNodes.add(new String[] {name, toRemoveAddress.getIp(),
				Integer.toString(toRemoveAddress.getPort())});

		for (ConfigCommInterface cs : uniqueConfigStores) {
			if (cs.getServerAddress().isSameAddress(toRemoveAddress)) {
				uniqueConfigStores.remove(cs);
				break;
			}
		}

		List<Integer> indexes = new ArrayList<>();
		int MAX = uniqueConfigStores.size();
		for (int i : indexesTmp) {
			indexes.add(i % MAX);
		}
		return indexes;
	}

	protected String printNodes() {
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

	protected String printPossible() {
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
