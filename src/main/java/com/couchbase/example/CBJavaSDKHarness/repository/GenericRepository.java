/*
 * Copyright 2015 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.example.CBJavaSDKHarness.repository;

import static com.couchbase.example.CBJavaSDKHarness.repository.RepositoryHelper.fromJsonDocument;
import static com.couchbase.example.CBJavaSDKHarness.repository.RepositoryHelper.toJsonDocument;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.example.CBJavaSDKHarness.CouchbaseConfig;
import com.couchbase.example.CBJavaSDKHarness.exception.RepositoryException;

public class GenericRepository  implements Repository<String> {
	private final static Logger LOGGER = 
		LoggerFactory.getLogger(GenericRepository.class);


	private Bucket bucket;
	
	public GenericRepository(String bucketName, String bucketPassword)
	{
	
		this.bucket = getBucket(bucketName,bucketPassword);
	}

	public String findById(String id) {
		if(isBlank(id)) {
			throw new IllegalArgumentException("id is blank");
		}
		JsonDocument doc = null;
		try {
			doc = bucket.get(id, 1, TimeUnit.SECONDS);
		} catch(CouchbaseException e) {
			throw new RepositoryException(e);
		}
		return fromJsonDocument(doc);
	}


	public String insert(String id, String source) {
		JsonDocument docIn = toJsonDocument(id, source);
		JsonDocument docOut = null;
		try {
			docOut = bucket.insert(docIn, 1, TimeUnit.SECONDS);
		} catch(CouchbaseException e) {
			throw new RepositoryException(e);
		}
		LOGGER.debug("INSERT: " + id);
		return fromJsonDocument(docOut);
	}

	public void delete(String id, String source) {
		JsonDocument doc = toJsonDocument(id, source);
		try {
			bucket.remove(doc, 1, TimeUnit.SECONDS);
		} catch(CouchbaseException e) {
			throw new RepositoryException(e);
		}
		LOGGER.debug("DELETE: " + id);
	}

	
	public String update(String id, String source) {
		JsonDocument docIn = toJsonDocument(id, source);
		JsonDocument docOut = null;
		try {
			docOut = bucket.replace(docIn, 1, TimeUnit.SECONDS);
		} catch(CouchbaseException e) {
			throw new RepositoryException(e);
		}
		LOGGER.debug("UPDATE: " + id);
		return fromJsonDocument(docOut);
	}

	
	public String upsert(String id, String source) {
		JsonDocument docIn = toJsonDocument(id, source);
		JsonDocument docOut = null;
		try {
			docOut = bucket.upsert(docIn, 1, TimeUnit.SECONDS);
		} catch(CouchbaseException e) {
			throw new RepositoryException(e);
		}
		LOGGER.debug("UPSERT: " + id);
		return fromJsonDocument(docOut);
	}

	public List<String> getQueryResultFor(String query, String parameter)
	{
		List<String> resultSet = new ArrayList<String>();
		 // Create a N1QL Primary Index (but ignore if it exists)
		// TODO Create appropriate Index based on your query and delete primary index before launching to production
	    bucket.bucketManager().createN1qlPrimaryIndex(true, false);

	    // Perform a N1QL Query
	    N1qlQueryResult result = bucket.query(
	        N1qlQuery.parameterized(query,
	        JsonArray.from(parameter))
	    );

	    // Print each found Row
	    for (N1qlQueryRow row : result) {
	        System.out.println(row);
	        resultSet.add(row.toString());
	    }
	    return resultSet;
	}

	private Bucket getBucket(String bucketName, String bucketPassword) {

		return CouchbaseConfig.getSingletonInstance().getBucket(bucketName, bucketPassword);
	}
	
	public void terminateSession()
	{
		CouchbaseConfig.getSingletonInstance().destroyInstance();
	}
	
	
	
}