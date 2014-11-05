package app_kvServer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Storage {
	
	public String get(String key){
		BufferedReader br;
		String line = "";
		try {
	        br = new BufferedReader(new FileReader("as"));
	        try {
	            while((line = br.readLine()) != null)
	            {
	                String[] words = line.split(" ");

	                  if (words[0].equals(key)) {
	                	  br.close();
	                	  return words[1];
	                }
	            }
	            br.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    } catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }
		return "No matching key in the storage server!";	
	}
	
	public void put(String Key, String Value){
		String newString = (Key + " " + Value);
		Singleton.getInstance().writeToFile(newString );	
	}	
}
