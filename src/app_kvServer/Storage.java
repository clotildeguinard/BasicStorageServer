package app_kvServer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;

public class Storage {
	private Logger logger = Logger.getRootLogger();
	
	public KVMessage put(String key, String value){
		StatusType status = StatusType.PUT_SUCCESS;
		String newString = (key + " " + value);
		try {
			if (get(key).getValue() != null) {
				status = StatusType.PUT_UPDATE;
				SingletonWriter.getInstance().overwriteInFile(newString );
			} else {
				SingletonWriter.getInstance().appendToFile(newString );
			} 
		} catch (IOException e) {
			SingletonWriter.getInstance().closeWriter();
			logger.error("A connection error occurred during writing", e); 
			return new KVMessageImpl(key, value, StatusType.PUT_ERROR);
		}
		return new KVMessageImpl(key, value, status);

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
				logger.error("A connection error occurred during reading", e);
	        }
	    } catch (FileNotFoundException e) {
			logger.error("The file could not be found", e);
	    }	
		return new KVMessageImpl(key, null, StatusType.GET_ERROR);
	}	
}
