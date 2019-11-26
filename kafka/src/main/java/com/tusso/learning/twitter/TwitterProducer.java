package com.tusso.learning.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class TwitterProducer {
	
	private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	private String consumerKey = "oC9Yotk2LuBy0Dw0Bz4eKeMRh";
	private String consumerSecret = "QE0UtS0zHea6PX6IA8cU1Q74KDx85oJEfDjemJEriTqNNCDapG";
	private String token = "2948337934-JTW1rnUZXWVsyBq0e8L7wvLRwFOkgIan6hFJEoH";
	private String secret = "jM7htNyzP9DcsNGYX9ogLc0q0Pbs0VL8EOjbtp8k5yQAc";
	
	List<String> terms = Lists.newArrayList("kafka", "bitcoin", "sport", "brazil");
	
	public TwitterProducer() {
	}
	
	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run() {
		logger.info("Setup");
		
		BlockingQueue<String> msgQueue = 
				new LinkedBlockingQueue<String>(100000);
		
		Client client = createTwitterClient(msgQueue);
		client.connect();
		
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		Runtime.getRuntime().addShutdownHook(new Thread(()-> {
			logger.info("Stopping application..");
			logger.info("sutting down client from twitter");
			client.stop();
			logger.info("closing producer..");
			producer.close();
			logger.info("done!");
		}));
		
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if(msg != null) {
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null) {
							logger.error("Something bad happened");
						}
						
					}
				});
			}
		}
		logger.info("End of Application");
	}
	
	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
	
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		
		return hosebirdClient;
	}
	
	public KafkaProducer<String, String> createKafkaProducer(){
		String bootstrapServer = "127.0.0.1:9092";
    	
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        //safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        
        // high thoughput producer 
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        
        return producer;
	}
}
