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

/**
 *  A mechanism for encapsulating storage, retrieval, and search behavior 
 *  that emulates a collection of objects.
 *  
 * @author Tony Piazza
 */
public interface Repository<T> {
	T findById(String id);

	T insert(String id, T source);

	T update(String id, T source);

	T upsert(String id, T source);

	void delete(String id, T source);
}