package app_kvServer;

import java.io.BufferedWriter;
import java.io.File;
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