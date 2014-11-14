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
		
	public static String[] getHashKeyBounds(String fileLocation, String serverIp, int serverPort) throws IOException {
		BufferedReader br = null;
		System.out.println(serverIp);
		try {
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

}
