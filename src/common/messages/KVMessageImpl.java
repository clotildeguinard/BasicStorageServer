package common.messages;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;



public class KVMessageImpl implements KVMessage {
	public String key;
	public String value;
	public StatusType statusType;

	public KVMessageImpl(String key, String value, StatusType statusType) {
		this.key = key;
		this.value = value;
		this.statusType = statusType;
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	@Override
	public String getKey(){
		return key;
	}
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	@Override
	public String getValue(){
		return value;
	}

	public byte[] serialize() {
		//TODO
	
		byte [] text= new byte[5];
		TextMessage q = new TextMessage(text);
		
		
        FileOutputStream fos = new FileOutputStream("c:\\temp.out");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(q);
        fos.close();

        FileInputStream fis = new FileInputStream("c:\\temp.out");
        ObjectInputStream oin = new ObjectInputStream(fis);
        TextMessage q2 = (TextMessage)oin.readObject();
        fis.close();
        
		return null;
	}

	
	public String marshal() {
		//TODO
		TextMessage q = new TextMessage ();
		
		{
		    q1.append("<status>"+ getStatus+ "</status>");
		    q1.append ("<key>"+getKey+"</key>");
		    q1.append ("<value>"+getValue+"</value");
		    
		   q1.append 
		
		   
		  return null;
		}
		   private static void marshalingExample() throws JAXBException
		   {
		       JAXBContext jaxbContext = JAXBContext.newInstance(TextMessage.class);
		       Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
		    
		       jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		        
		       //Marshal the key list in console
		       jaxbMarshaller.marshal(q, System.out);
		        
		       //Marshal the keys list in file
		       jaxbMarshaller.marshal(q, new File("c:/temp/keys.xml"));
		   }

	}

	}

	/**
	 * @return the status type that is associated with this message, 
	 * 		null if not value is associated.
	 */
	@Override
	public StatusType getStatus() {
		return statusType;
	}
}
