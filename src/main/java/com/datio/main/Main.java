package main.java.com.datio.main;

import main.java.com.datio.producers.Producer;

/**
 * Created by jorge on 27/07/16.
 */
public class Main {
    public static void main(String[] args){
        Producer producer = new Producer();
        producer.produce();
        producer.close();


    }
}
