package app_kvServer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.cache_strategies.Pair;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;

public class Storage implements Iterable<Pair<String, String>>{
	private Logger logger = Logger.getRootLogger();
    private File file;
    private File bisFile;
    private final static String EOL = System.getProperty("line.separator");
    
    public Storage(String rootPath) throws IOException {
    	String storageFile = rootPath + "storage.txt";
    	String storageFileBis = rootPath + "storage_bis.txt";
    	
    	file = new File(storageFile);
    	file.createNewFile();
    	erase(storageFile);
    	
    	bisFile = new File(storageFileBis);
    	bisFile.createNewFile();
    	erase(storageFileBis);
    }
    
    private void erase(String fileName) throws IOException {
    	FileOutputStream writer = new FileOutputStream(fileName);
    	writer.write((new String()).getBytes());
    	writer.close();
    }

	public KVMessage put(String key, String value) throws IOException{
		StatusType status = StatusType.PUT_SUCCESS;
		if (!value.equals("null")) {
			String newString = (key + " " + value + EOL);
			SingletonWriter.getInstance().initializeAppendingWriter(file);
			try {
				if (get(key).getValue() != null) {
					status = StatusType.PUT_UPDATE;
					overwriteInFile(key, value );
				} else {
					SingletonWriter.getInstance().write(newString);
				} 
			} catch (IOException e) {
				try {
					SingletonWriter.getInstance().closeWriter();
				} catch (IOException e1) {
					logger.error("An io error occurred when closing writer", e1);
				}
				logger.error("An io error occurred during writing", e); 
				return new KVMessageImpl(key, value, StatusType.PUT_ERROR);
			} finally {
				SingletonWriter.getInstance().closeWriter();
			}
		}
		return new KVMessageImpl(key, value, status);

	}
	
	private void overwriteInFile(String key, String value) throws FileNotFoundException, IOException {
		String line;
		String newString = (key + " " + value + EOL);
		SingletonWriter.getInstance().initializeAppendingWriter(bisFile);
		BufferedReader br = new BufferedReader(new FileReader(file));

		while((line = br.readLine()) != null) {

			String[] words = line.split(" ");

			if (words[0].equals(key)) {
				if (!value.equals("null")) {
					SingletonWriter.getInstance().write(newString);
				}
			} else {
				SingletonWriter.getInstance().write(line + EOL);
			}
		}
		br.close();
		SingletonWriter.getInstance().closeWriter();
		File old = file;
		file = bisFile;
		bisFile = old; 
	}

	public KVMessage get(String key) throws FileNotFoundException, IOException{

		String line;
		BufferedReader br = new BufferedReader(new FileReader(file));

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

	@Override
	public Iterator<Pair<String, String>> iterator() {
		try {
			return new Iterator<Pair<String,String>>() {
				String line;
				BufferedReader br = new BufferedReader(new FileReader(file));
				
				@Override
				public boolean hasNext() {
						try {
							return ((line = br.readLine()) != null);
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							br.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
						return false;
				}
				
				@Override
				public Pair<String, String> next() {
					String[] words = line.split(" ");
					return new Pair<String, String>(words[0], words[1]);
				}
				
				@Override
				public void remove() {
					// TODO Auto-generated method stub
					
				}
			};
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}	
}
