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

            System.out.println("Sending messages...");
            for (int i = 0; i < 1000000; i++) {
                json.put("name", "name"+i);
                json.put("surname", "surname"+i);
                json.put("age", i);
                json.put("office", 1);

                //System.out.println("Node "+ i);
                //System.out.println(json.toString());
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("clients", json.toString());
                //producer.send()
                producer.send(record);
                producer.flush();
                System.out.println("Enviado "+ i);
            }
        } catch (Exception throwable) {
            System.out.println(throwable.getStackTrace());
        }
        System.out.println("Messages send");
    }

    public void close(){
       producer.close();
    }

}
