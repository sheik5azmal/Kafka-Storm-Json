package producer;

import java.util.Properties;
import java.util.Random;

import org.json.JSONObject;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class JsonProducer 
{
	 private static final Integer NUMBER_OF_FIELDS = 2;
	    /** Constant: The name. **/
	    private static int thermostat_ID;
	    /** Constant: The type. **/
	    private static int temp;
	   private static String generateJson()
	   {
		   Random rnd = new Random();
	    	JSONObject tuple = new JSONObject();
		    	thermostat_ID= rnd.nextInt(100);
		    	temp=rnd.nextInt(1000);
		    	tuple.put("thermostat_ID", thermostat_ID);
		    	tuple.put("temp", temp);
		       return tuple.toString();
	   }
	public static void main(String[] args) 
	{
        int record;
        
        
        Properties props = new Properties();
        props.put("metadata.broker.list", "172.31.58.177:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "main.java.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
        Random rnd = new Random();
    	JSONObject tuple = new JSONObject();
    for(int i=1;i<=record;i++)
    {
    	KeyedMessage<String, String> data = new KeyedMessage<String, String>("testJson",generateJson(i));
        producer.send(data);
    }
    producer.close();    
}
}
