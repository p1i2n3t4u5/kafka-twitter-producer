package com.kafka;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

@SpringBootApplication
public class KafkaTwitterProducerApplication implements CommandLineRunner {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	private static final Logger logger = LoggerFactory.getLogger(ServletInitializer.class);

	public static void main(String[] args) {
		SpringApplication.run(KafkaTwitterProducerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		logger.info("started--------");
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

		Client client = this.createTwitterClient(msgQueue, eventQueue);
		// Attempts to establish a connection.
		client.connect();

		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
				client.stop();
			}

			if (msg != null) {
				logger.info(msg);
				ListenableFuture<SendResult<String, String>> f = kafkaTemplate.send("twittertweets", null, msg);
				f.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

					@Override
					public void onSuccess(SendResult<String, String> result) {
						logger.info("Product Record:" + result.getProducerRecord());
						logger.info("Metadata:Offset:" + result.getRecordMetadata().offset());
						logger.info("Metadata:Partition:" + result.getRecordMetadata().partition());
						logger.info("Metadata:Topic:" + result.getRecordMetadata().topic());
					}

					@Override
					public void onFailure(Throwable ex) {
						logger.error(ex.getMessage());
					}
				});
			}
		}

		client.stop();
		logger.info("ended--------");
	}

//	private String consumerKey = "cvkJYSgjmLsWnRki8ZrcIqIMq";
//	private String consumerSecret = "PxkhRksyJRmPxm9MuncTovrrFynJ0GJY4d8btvBipyCiA3FEnN";
//	private String token = "1159409955536982016-E1LnEyRNaNrW59QSz8rPyteZ6Q9iWs";
//	private String secret = "8oTLQ6IvCIiL6bLXtkbglGjb0j7gZGu0QmE9YaxY1C0Qn";
//	
	
	private String consumerKey = "";
	private String consumerSecret = "";
	private String token = "";
	private String secret = "";
	
	
	private List<String> terms = Lists.newArrayList("modi");

	public Client createTwitterClient(BlockingQueue<String> msgQueue, BlockingQueue<Event> eventQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);
		
		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue)).eventMessageQueue(eventQueue); // optional: use this
																									// if you want to
																									// process client
																									// events

		Client hosebirdClient = builder.build();

		return hosebirdClient;

	}

}
