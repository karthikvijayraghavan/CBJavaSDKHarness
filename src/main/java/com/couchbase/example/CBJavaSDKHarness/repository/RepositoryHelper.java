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

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import static org.apache.commons.lang3.StringUtils.isBlank;


import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import com.couchbase.example.CBJavaSDKHarness.exception.RepositoryException;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class RepositoryHelper {
	private final static JsonTranscoder TRANSCODER = new JsonTranscoder();
	private final static ObjectMapper MAPPER = new ObjectMapper();
	private final static String CAS_KEY = "cas";

	static {
		MAPPER.disable(FAIL_ON_UNKNOWN_PROPERTIES);
	}

	public static <T> T fromJsonObject(JsonObject source, Class<T> type) {
		try {
			return MAPPER.readValue(TRANSCODER.jsonObjectToString(source), type);
		} catch(Exception e) {
			throw new RepositoryException(e);
		}
	}

	/**
	 * Utility method to convert the specified JsonDocument into a JSON String.
	 * 
	 * @param source JsonDocument to be converted
	 * @return String representation of JsonDocument source
	 */
	public static String fromJsonDocument(JsonDocument source) {
		String json = null;
		if(source != null) {
			JsonObject content = source.content();
			content.put(CAS_KEY, source.cas());
			try {
				json = TRANSCODER.jsonObjectToString(content);
			} catch (Exception e) {
				throw new RepositoryException(e);
			}
		}
		return json;
	}

	/**
	 * Utility method to convert the specified JSON String into a JsonDocument
	 * representation and set the CAS value (if found).
	 * 
	 * @param id String ID of resulting document
	 * @param source String to be converted
	 * @return JsonDocument representation of String source
	 */
	public static JsonDocument toJsonDocument(String id, String source) {
		if(isBlank(id)) {
			throw new IllegalArgumentException("id is blank");
		}
		if(isBlank(source)) {
			throw new IllegalArgumentException("source is blank");
		}
		try {
			JsonObject content = TRANSCODER.stringToJsonObject(source);
			JsonDocument doc = null;
			Long cas = content.getLong(CAS_KEY);
			if(cas == null) {
				doc = JsonDocument.create(id, content);
			} else {
				content.removeKey(CAS_KEY);
				doc = JsonDocument.create(id, content, cas);
			}
			return doc;
		} catch (Exception e) {
			throw new RepositoryException(e);
		}
	}
}