package app_kvServer;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;

public class Singleton {
    private static final Singleton inst= new Singleton();

    private Singleton() {
        super();
    }

    public synchronized void writeToFile(String str) {
    	Writer writer = null;

    	try {
    	    writer = new BufferedWriter(new OutputStreamWriter(
    	          new FileOutputStream("filename.txt"), "utf-8"));
    	    writer.write(str);
    	} catch (IOException ex) {
    	} finally {
    	   try {writer.close();} catch (Exception ex) {}
    	}
    }

    public static Singleton getInstance() {
        return inst;
    }

}