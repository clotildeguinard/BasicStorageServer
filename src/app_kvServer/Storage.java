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
				overwriteInFile(key, newString );
			} else {
				SingletonWriter.getInstance().appendToFile(newString );
			} 
		} catch (IOException e) {
			try {
				SingletonWriter.getInstance().closeWriter();
			} catch (IOException e1) {}
			logger.error("A connection error occurred during writing", e); 
			return new KVMessageImpl(key, value, StatusType.PUT_ERROR);
		}
		return new KVMessageImpl(key, value, status);

	}
	
	private void overwriteInFile(String key, String newString) throws FileNotFoundException, IOException {
		String line;
		BufferedReader br = new BufferedReader(new FileReader("storage.txt"));

				while((line = br.readLine()) != null) {
					String[] words = line.split(" ");

					if (words[0].equals(key)) {
						SingletonWriter.getInstance().appendToFile(newString);
					} else {
						SingletonWriter.getInstance().appendToFile(line);
					}
					br.close();
				}
	}

	public KVMessage get(String key) throws FileNotFoundException, IOException{

		String line;
		BufferedReader br = new BufferedReader(new FileReader("storage.txt"));
		while((line = br.readLine()) != null)
		{
			String[] words = line.split(" ");

			if (words[0].equals(key)) {
				br.close();
				return new KVMessageImpl(key, words[1], StatusType.GET_SUCCESS);
			}
		}
		br.close();
		return new KVMessageImpl(key, null, StatusType.GET_ERROR);
	}	
}
