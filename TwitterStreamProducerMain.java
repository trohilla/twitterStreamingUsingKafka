package com.tushar.kafka.twitter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author tushar
 *
 */
public class TwitterStreamProducerMain {

	private static TwitterStreamProducer producerService = new TwitterStreamProducer();
	private static TwitterStream stream = new TwitterStream();

	private static ExecutorService executor = null;
	private static volatile Future taskOneResults = null;
	private static volatile Future taskTwoResults = null;
	
	public TwitterStreamProducerMain() {
		executor = Executors.newFixedThreadPool(2);
	}

	public static void startProducerAndStream(String filter) {
		if (taskOneResults == null || taskOneResults.isDone() || taskOneResults.isCancelled()) {
			taskOneResults = executor.submit(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							producerService.run(filter);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						try {
							Thread.sleep(1000);
						} catch (Throwable e) {
							e.printStackTrace();
						}
					}

				}
			});
		}

		if (taskTwoResults == null || taskTwoResults.isDone() || taskTwoResults.isCancelled()) {
			taskTwoResults = executor.submit(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							stream.start(filter);
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						try {
							Thread.sleep(1000);
						} catch (Throwable e) {
							e.printStackTrace();
						}
					}

				}
			});
		}
	}
	
}
