package com.github.heet1996.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {

    public static void main(String[] args) {
            //Create a logger
            Logger logger= LoggerFactory.getLogger(ConsumerDemoThread.class.getName());

            String bootstrapserver="localhost:9092";
            String groupId="my_first_application";
            String topic="first_topic";
            CountDownLatch latch=new CountDownLatch(1);
            ConsumerRunnable cr=new ConsumerRunnable(bootstrapserver,groupId,topic,latch);
            //add a consumer thread
            Thread consumerThread=new Thread(cr);
            //start the consumer thread
            consumerThread.start();
            //add a shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(()->{
                    logger.info("Caught the shutdown hook");
                    cr.shutdown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("Application exited");
            }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted",e);
        }
        finally {
            logger.info("Application is close..");


        }
    }

}
class ConsumerRunnable implements Runnable
{
    private String bootstrapserver;
    private String groupId;
    private String topic;
    CountDownLatch latch;
    KafkaConsumer<String,String> consumer;
    //Create a logger
    Logger logger= LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    public ConsumerRunnable(String bootstrapserver,
                            String groupId,
                            String topic,
                            CountDownLatch latch)
    {

        this.bootstrapserver=bootstrapserver;
        this.groupId=groupId;
        this.topic=topic;
        this.latch=latch;


    }
    @Override
    public void run() {
        //Create the config
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //create a consumer
        consumer=new KafkaConsumer<String, String>(properties);
        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        try {
            while(true)
            {
                ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String,String> record:records) {
                    logger.info("Key :"+record.key()+", Value"+record.value());
                }
            }
        }
        catch (WakeupException e)
        {
            logger.error("Shutting down the consumer",e);
        }
        finally {
            consumer.close();
            latch.countDown();
        }


    }
    public void shutdown()
    {
        consumer.wakeup();

    }
}