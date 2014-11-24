package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.swing.text.Utilities;

import app_kvServer.KVServer;

public class ECSClient {
	
	protected InputStream    fis;
	protected BufferedReader br;
	protected String         line;
	private final String metadataLocation = "./src/app_kvServer/metadata.txt";
	private List<String> List = new ArrayList<String>(); 
	private int numberOfUsedNodes = 0;
	
	public void initService(int numberOfNodes) throws IOException, NoSuchAlgorithmException{
		fis = new FileInputStream("ecs.config.txt");
		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
		
		
		while ((line = br.readLine()) != null && numberOfNodes != 0) {
			String[] words = line.split(" ");
			
			int Port = Integer.parseInt(words[2]);
			String IpAndPort = new StringBuilder(words[1]).append(";").append(words[2]).toString();

			//combine ip and port and convert to hash.
			String hashedKey = new BigInteger(1,MessageDigest.getInstance("MD5").digest(IpAndPort.getBytes("UTF-8"))).toString(16);
			List.add(words[0]);
		    List.add(words[1]);
		    List.add(words[2]);
			List.add(hashedKey);	

//			KVServer words[0] = new KVServer(Port);
//			kvServer.initKVServer("", cacheSize, strategy);			
		}
		
		Sort();
		createMetaData();
	
		numberOfUsedNodes = numberOfNodes + 1;
		br.close();		
		br = null;
		fis = null;
	}
	
	public void start(){
		
	}
	
	public void stop(){
		
	}
	
	public void shutdown(){
		
	}
	
	public void addNode() throws IOException, NoSuchAlgorithmException{
		fis = new FileInputStream("ecs.config.txt");
		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
		
		for(int i = 0; i <= numberOfUsedNodes; ++i)
			  br.readLine();
		
		String[] words = br.readLine().split(" ");
		String IpAndPort = new StringBuilder(words[1]).append(";").append(words[2]).toString();

		//combine ip and port and convert to hash.
		String hashedKey = new BigInteger(1,MessageDigest.getInstance("MD5").digest(IpAndPort.getBytes("UTF-8"))).toString(16);
		List.add(words[0]);
	    List.add(words[1]);
	    List.add(words[2]);
		List.add(hashedKey);
		
		Sort();
		createMetaData();
		
		br.close();		
		br = null;
		fis = null;
	}
	
	public void removeNode(int whichOne) throws IOException{
		fis = new FileInputStream("ecs.config.txt");
		
		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
		for(int i = 0; i <= whichOne; ++i)
			  br.readLine();
		
		String[] words = br.readLine().split(" ");
		int Size = List.size();		
		List<String> removedList = new ArrayList<String>();

		for(int i = 0; i < Size; i++){
			if(words[0].equals(List.get(i))){
				removedList.add(List.get(i));
				removedList.add(List.get(i+1));
				removedList.add(List.get(i+2));
				removedList.add(List.get(i+3));
			}
		}
		
		List.removeAll(removedList);
		Sort();
		createMetaData();
		
		br.close();		
		br = null;
		fis = null;
	}
	
	private void Sort(){
		int Size = List.size();
		String current;
		
		for (int i = 0; i < Size ; i+=4){
			String max = List.get(1);
			int indexOfMax = 0;
			for (int j = 3; j < Size-i; j+=4){
				current = List.get(i);
				if(current.compareTo(max) >= 0){
					max = current;
					indexOfMax = j;
				}
			}
			Collections.swap(List, indexOfMax, Size-i);
			Collections.swap(List, indexOfMax-1, Size-(i+1));
			Collections.swap(List, indexOfMax-2, Size-(i+2));
			Collections.swap(List, indexOfMax-3, Size-(i+3));
		}				
	}
	
	private void createMetaData() throws FileNotFoundException, UnsupportedEncodingException{
		int Size = List.size();
		int n = 4;
		PrintWriter writer = new PrintWriter(metadataLocation, "UTF-8");
		writer.println("Name,      IP,            PORT,        StartIndex,             EndIndex" );
		writer.println(List.get(0) + " ; " + List.get(1) + " ; " + List.get(2) +  " ; 0000000000000000" + " ; " + List.get(3));
		while (Size != n){
			writer.println(List.get(n) + " ; " + List.get(n+1) + " ; " + List.get(n+2) + " ; " + List.get(n-1) + " ; " + List.get(n+3));
			n+=4;
		}		
		writer.close();
	}
	
	
	
	
}
