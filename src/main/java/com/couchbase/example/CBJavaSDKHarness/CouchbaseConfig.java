package com.couchbase.example.CBJavaSDKHarness;

import java.io.IOException;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class CouchbaseConfig {

	private Cluster cluster;
	private static Map<String, Bucket> bucketMap = null;

	private static CouchbaseConfig singletonInstance;

	private CouchbaseConfig() {
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
				// this sets the IO socket timeout globally
				.socketConnectTimeout((int) TimeUnit.SECONDS.toMillis(1)).kvTimeout(TimeUnit.SECONDS.toMillis(1))
				.queryTimeout(TimeUnit.SECONDS.toMillis(1))
				/**
				 * this sets the connection timeout for openBucket calls
				 * globally (unless a particular call provides its own timeout)
				 **/
				.connectTimeout(TimeUnit.SECONDS.toMillis(5)).build();

		String nodeList = getPropertiesFile().getProperty("nodeList");
		List<String> nodes = Arrays.asList(nodeList.split("\\s*,\\s*"));

		cluster = CouchbaseCluster.create(env, nodes);
		String bucketName = getPropertiesFile().getProperty("defaultBucket");
		Bucket bucket = cluster.openBucket(bucketName, 240L, TimeUnit.SECONDS);
		bucketMap = new HashMap<String, Bucket>();
		bucketMap.put(bucketName, bucket);
	}

	/*
	 * In the below code snippet imagine that multiple threads comes
	 * concurrently and tries to create the new instance. In such situation
	 * there may be three or more threads are waiting on the synchronized block
	 * to get access. Since we have used synchronized only one thread will be
	 * given access. All the remaining threads which were waiting on the
	 * synchronized block will be given access when first thread exits this
	 * block. However when the remaining concurrent thread enters the
	 * synchronized block they are prevented to enter further due to the double
	 * check : null check. Since the first thread has already created an
	 * instance no other thread will enter this loop. All the remaining threads
	 * that were not lucky to enter the synchronized block along with the first
	 * thread will be blocked at the first null check. This mechanism is called
	 * double checked locking and it provides significant performance benefit
	 * and also it is cost effective solution.
	 * 
	 */
	public static CouchbaseConfig getSingletonInstance() {
		if (null == singletonInstance) {
			synchronized (CouchbaseConfig.class) {
				if (null == singletonInstance) {
					singletonInstance = new CouchbaseConfig();
				}
			}
		}
		return singletonInstance;
	}

	public Bucket getBucket(String name, String password) {
		if (null != bucketMap && bucketMap.get(name) == null) {
			synchronized (CouchbaseConfig.class) {
				Bucket bucket;
				if (null != bucketMap && bucketMap.get(name) == null) {
					if (null != password) {
						bucket = cluster.openBucket(name, password, 240L, TimeUnit.SECONDS);
					} else {
						bucket = cluster.openBucket(name, 240L, TimeUnit.SECONDS);
					}
					bucketMap.put(name, bucket);
				}
			}

		}
		return null != bucketMap ? bucketMap.get(name) : null;
	}

	public static Properties getPropertiesFile() {
		Properties prop = new Properties();
		InputStream input = null;

		try {

			String filename = "application.properties";
			input = CouchbaseConfig.class.getClassLoader().getResourceAsStream(filename);
			if (input == null) {
				System.out.println("Sorry, unable to find " + filename);
				return null;
			}

			// load a properties file from class path, inside static method
			prop.load(input);

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return prop;
	}

	public void destroyInstance() {
		if (null != bucketMap && !bucketMap.isEmpty()) {
			Iterator<String> bucketMapIter = bucketMap.keySet().iterator();
			while (bucketMapIter.hasNext()) {
				Bucket bucket = bucketMap.get(bucketMapIter.next());
				bucket.close();
			}
			cluster.disconnect();
		}
		singletonInstance=null;
	}
}
