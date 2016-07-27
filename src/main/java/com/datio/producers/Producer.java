package main.java.com.datio.producers;


import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by jorge on 27/07/16.
 */
public class Producer {


    public Producer(){

    }

    public void produce(){
        try {
            KafkaProducer<String, String> producer;
            try (InputStream props = Resources.getResource("producer.props").openStream()) {
                Properties properties = new Properties();
                properties.load(props);
                producer = new KafkaProducer<>(properties);
            }
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
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("clients", json.toString(), json.toString());
                /*producer.send(new ProducerRecord<String, String>(
                        "fast-messages",
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));*/
                RecordMetadata metadata = producer.send(record).get();
                producer.flush();

                System.out.println("Enviado "+ i);
                Thread.sleep(20);

            }
            producer.close();
        } catch (Exception throwable) {
            throwable.printStackTrace();
        }
        System.out.println("Messages send");
    }

    public void close(){

    }

}
