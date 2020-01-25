package com.github.heet1996.kafka.tutorial1;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    String consumerKey="jW7tTlFbzsJlYuXzaavvmjObX";
    String consumerSecret="DDk9UgJdY5xweviqU2sNEfy10fmMVnZ4ofzEHI7kHiSibmlLym";
    String token="4537781100-WJjBjS4pTkszHBt0Bt4CPyG2z0Mu2f2GXR9XmrG";
    String secretToken="Kz05wSBSmbp3OSFreV6YlHIDmlEDE31OjCktCNRBHKk4F";
    Logger logger= LoggerFactory.getLogger(TwitterProducer.class);
    String bootStrapServer="localhost:9092";


    public static void main(String[] args) {
                new TwitterProducer().run();
    }
    public void run()  {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        //Create a client
        Client hosebirdClient= createTwitterClient(msgQueue);
        hosebirdClient.connect();
        //Create a kafka producer
        KafkaProducer<String, String> producer = createProducer();



        //Read the messages
        while (!hosebirdClient.isDone()) {
            String msg=null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }
            if(msg!=null)
            {
                //Create record
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_topic",null, msg);
                //send the data
                producer.send(record,
                        new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if(exception!=null)
                                {
                                    logger.error(exception.getMessage());
                                }
                            }
                        });
            }

        }
        producer.close();
    }
    public Client createTwitterClient(BlockingQueue<String> msgQueue)
    {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
//hosebirdEndpoint.followings(followings);

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secretToken);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        Client hosebirdClient = builder.build();
// Attempts to establish a connection.
        return hosebirdClient;
    }
    public KafkaProducer<String,String> createProducer()
    {
        //properties file
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,Integer.toString(5));
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;

    }

}
