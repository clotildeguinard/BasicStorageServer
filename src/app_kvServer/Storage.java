package app_kvServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.Iterator;

import org.apache.log4j.Logger;

import app_kvServer.cache_strategies.Pair;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;

public class Storage implements Iterable<Pair<String, String>>{
	private Logger logger = Logger.getLogger(getClass().getSimpleName());
    private File file;
    private File bisFile;
    private final static String EOL = System.getProperty("line.separator");
    private final static int MAX_TRIALS = 3;
    private final static String storageName = "storage.txt";
    private final static String storageBisName = "storage_bis.txt";
    
    public Storage(String rootPath, String ID) throws IOException {
    	IOException ex = null;
    	for (int i=0 ; i<MAX_TRIALS ; i++) {
    		try {
    			initStorage(rootPath + ID + "_");
    			return;
    		} catch (IOException e) {
    			ex = e;
    		}
    	}
    	logger.fatal("Storage could not be initialized after " + MAX_TRIALS + " trials.");
    	throw(ex);
    }
    
    private void initStorage(String rootPath) throws IOException {
    	String storageFile = rootPath + storageName;
    	String storageFileBis = rootPath + storageBisName;
    	
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

	public KVMessage put(String key, String value) throws FileNotFoundException, IOException{
		StatusType status = StatusType.PUT_SUCCESS;
		if (value.equals("null")) {
			return new KVMessageImpl(key, value, status);
		} else {
			String newString = (key + " " + value + EOL);
			SingletonWriter.getInstance().initializeAppendingWriter(file);
			try {
				if (get(key).getValue() != null) {
					status = StatusType.PUT_UPDATE;
					overwriteInFile(key, value );
				} else {
					SingletonWriter.getInstance().write(newString);
				} 
				return new KVMessageImpl(key, value, status);
			} finally {
				SingletonWriter.getInstance().closeWriter();
			}
		}

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
					// do nothing
				}
			};
		} catch (FileNotFoundException e) {
			logger.fatal("Storage file could not be found in its location.");
		}
		return null;
	}
	
	private static class SingletonWriter {
	    private static final SingletonWriter inst= new SingletonWriter();
	    private Writer writer;

	    private SingletonWriter() {
	        super();
	    }
	    public void initializeAppendingWriter( File f) throws UnsupportedEncodingException, FileNotFoundException {
	    	 writer = new BufferedWriter(new OutputStreamWriter(
	   	          new FileOutputStream(f, true), "utf-8"));
	    }

	    public synchronized void write(String str) throws IOException {
	    	 writer.write(str);
	    }

	    public void closeWriter() throws IOException {
	    	if (writer != null) {
					writer.close();
	    	}
	    }
	    
	    public static SingletonWriter getInstance() {
	        return inst;
	    }
	}
}
