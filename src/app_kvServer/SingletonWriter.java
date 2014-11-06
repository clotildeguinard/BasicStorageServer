package app_kvServer;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

public class SingletonWriter {
    private static final SingletonWriter inst= new SingletonWriter();
    private Writer writer;

    private SingletonWriter() {
        super();
    }

    public synchronized void appendToFile(String str) throws IOException {
    	    writer = new BufferedWriter(new OutputStreamWriter(
    	          new FileOutputStream("storage.txt"), "utf-8"));
    	    writer.write(str);
    }

    public void closeWriter() {
    	if (writer != null) {
    		try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
    

    public static SingletonWriter getInstance() {
        return inst;
    }

	public void overwriteInFile(String newString) {
		// TODO Auto-generated method stub
		
	}

}