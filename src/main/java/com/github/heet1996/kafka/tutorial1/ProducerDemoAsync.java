package com.github.heet1996.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerDemoAsync {
    public static void main(String[] args) {
        String bootstrapServer = "localhost:9092";
        Logger logger= LoggerFactory.getLogger(ProducerDemoAsync.class);
        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create record
        List<ProducerRecord<String,String>> records = new ArrayList<ProducerRecord<String,String>>();
        for(int i=0;i<10;i++) {
            records.add(new ProducerRecord<String,String>("first_topic",Integer.toString(i),"record"+i));
        }

        //send the data
        records.forEach((record)->{
            producer.send(record,(recordData, exception)->{
                if(exception==null) {

                    logger.info("Kafka started producing the message : \n" +
                            "The key is : " + record.key() + "\n" +
                            "The message is : " + record.value() + "\n" +
                            "The topic is : " + recordData.topic() + "\n" +
                            "The partition is : " + recordData.partition() + "\n" +
                            "The offset is : " + recordData.offset() + "\n"
                    );
                }
                else logger.error("The error is :"+exception);
            });
            //flush the producer
            producer.flush();
            //flush and close the producer
        });

        producer.close();
    }
}
