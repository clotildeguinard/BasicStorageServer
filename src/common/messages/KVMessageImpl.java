package common.messages;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import common.messages.Main.Q;

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
	

		TextMessage q = new TextMessage(byte[]);
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
		static TextMessage q = new TextMessage q();
		
		{
		    q.setTextMessage(new ArrayList<TextMessage>());
		    //Create 1st message
		    TextMessage q1 = new TextMessage();
		    q1.setId(1);
		    q1.setKey("Key");
		    q1.setValue("Value");
		   
		     

		   q.getTextMessage().add(q1);
		   
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
