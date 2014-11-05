package app_kvServer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;

public class Storage {
	
	public KVMessage put(String key, String value){
		//TODO
		String newString = (key + " " + value);
		Singleton.getInstance().writeToFile(newString );
		// write to file
		// adapt status type (return error if exception thrown, update if overwriting) 
		return new KVMessageImpl(key, value, StatusType.PUT_SUCCESS);

	}
	
	public KVMessage get(String key){

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
	                	  return new KVMessageImpl(key, words[1], StatusType.GET_SUCCESS);
	                }
	            }
	            br.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    } catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }	
		return new KVMessageImpl(key, null, StatusType.GET_ERROR);
	}	
}
