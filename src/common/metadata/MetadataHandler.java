package common.metadata;

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

public class MetadataHandler {
	private final static String fieldSeparator = ";";
	
	/**
	 * overwrite metadata file with more recent metadata
	 * @param metadata
	 * @param fileLocation
	 * @throws IOException
	 */
	public static void updateFile (String metadata, String fileLocation) throws IOException {
		Writer writer = null;
		try  {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileLocation), "utf-8"));
			writer.write(metadata);
		} finally {
			if (writer != null) {
				writer.close();
			}
		}
				
	}
		
	/**
	 * 
	 * @param fileLocation
	 * @param serverIp
	 * @param serverPort
	 * @return the hashkey bounds for the given server
	 * @throws IOException
	 */
	public static String[] getHashKeyBounds(String fileLocation, String serverIp, int serverPort) throws IOException {
		BufferedReader br = null;
		try {
			System.out.println(serverIp);
			String port = Integer.toString(serverPort);
			br = new BufferedReader(new FileReader(fileLocation));
			String line;
			while((line = br.readLine()) != null)
			{
				String[] words = line.split(fieldSeparator);
// TODO
				// fix this
//				if (true) {
				if (words[0].equals(serverIp) && words[1].equals(port)) {
					br.close();
					return new String[] {words[2], words[3]};
				}
			}
			return null;
		} finally {
			if (br != null) {
				br.close();
			}
		}
	}
	
	/**
	 * 
	 * @param fileLocation : the location of the metadata
	 * @param key : the key we are interested in
	 * @return array with server ip and port responsible for key
	 * @throws IOException
	 */
	public static String[] getServerForKey(String fileLocation, String key) throws IOException {
		return null;

// TODO
	}

}
