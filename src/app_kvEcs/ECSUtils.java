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
import java.util.List;

import org.apache.log4j.Logger;

public class ECSUtils {
	protected static List<String[]> possibleRemainingNodes;
	protected static List<String> sortedNodeHashes;
	protected static List<ConfigCommInterface> sortedConfigStores;
	private final static Logger logger = Logger.getLogger(ECSUtils.class);
	private final static String hashingAlgorithm = "MD5";

	protected ECSUtils() {}

	protected void readConfigFile(String configLocation) {
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

	protected void launchSSH(String hostIp, String port, String logLevel) {
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

	protected void addNodeToLists(String[] nodeData)
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

	protected void sortNodeLists(){
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

	protected int findCsIndex(String nodeName) {
		int max = sortedNodeHashes.size();
		for (int i=0; i<max; i += 4) {
			if (sortedNodeHashes.get(i).equals(nodeName)) {
				return i/4;
			}
		}
		return -1;
	}


	protected ConfigCommInterface removeNodeFromLists(int index) {

		String name = sortedNodeHashes.remove(index * 4); // remove name
		String ip = sortedNodeHashes.remove(index * 4); // remove ip
		String port = sortedNodeHashes.remove(index * 4); // remove port
		sortedNodeHashes.remove(index * 4); // remove hash

		possibleRemainingNodes.add(new String[] {name, ip, port});

		return sortedConfigStores.remove(index);
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
