package app_kvServer.cache_strategies;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import common.messages.KVMessage;

import app_kvServer.DataCache;
import app_kvServer.Singleton;

public class FIFO implements DataCache{

	public FIFO(int capacity) {
		//TODO
	}
	@Override
	public KVMessage get(String Key) {
		//TODO
		return null;}


	public String getBis(String Key) {
		BufferedReader br;
		String line = "";
		try {
	        br = new BufferedReader(new FileReader("as"));
	        try {
	            while((line = br.readLine()) != null)
	            {
	                String[] words = line.split(" ");

	                for (String word : words) {
	                  if (word.equals(Key)) {

	                  }
	                }
	            }
	            br.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    } catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }
		return line;
		
	}

	@Override
	public KVMessage put(String Key, String Value) {
		//TODO
		return null;
	}

	public void putBis(String Key, String Value) {
		String newString = (Key + " " + Value);
		Singleton.getInstance().writeToFile(newString );		
	}
	

}


