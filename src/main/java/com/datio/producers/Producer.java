package main.java.com.datio.producers;


import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by jorge on 27/07/16.
 */
public class Producer {
    KafkaProducer<String, String> producer;

    public Producer(){
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void produce(){
        try {
            /*
                topic: client
                fields = name, surname, age, office
             */
            JSONObject json = new JSONObject();


            for (int i = 0; i < 1000000; i++) {
                json.put("name", "name"+i);
                json.put("surname", "surname"+i);
                json.put("age", i);
                json.put("office", 1);

                //System.out.println("Node "+ i);
                //System.out.println(json.toString());
                //ProducerRecord<String, String> record = new ProducerRecord<String, String>("fast-messages", json.toString());
                //producer.send()
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        }
    }

    public void close(){
       producer.close();
    }

}
