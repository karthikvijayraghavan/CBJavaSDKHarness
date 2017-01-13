package com.couchbase.example.CBJavaSDKHarness;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.example.CBJavaSDKHarness.repository.GenericRepository;

public class Example {
	private final static Logger LOGGER = LoggerFactory.getLogger(Example.class);

	public static void main(String... args) {
		GenericRepository repo = null;
		try {
			// Instantiate the repository with appropriate bucket name. This
			// will open the bucket that can be used for the rest of the
			// operations.
			
			String bucketName = CouchbaseConfig.getPropertiesFile().getProperty("defaultBucket");
			repo = new GenericRepository(bucketName, null);

			// Create a JSON Document
			JsonObject arthur = JsonObject.create().put("name", "Arthur").put("email", "kingarthur@couchbase.com")
					.put("interests", JsonArray.from("Holy Grail", "African Swallows"));

			// Store the Document
			String id = "u:king_arthur 10";
			repo.insert(id, arthur.toString());
			// Load the Document and print it
			System.out.println(repo.findById(id));

			// Update the Document
			arthur.put("address", "143 Wonderland, Heaven road, Above the world");
			repo.update(id, arthur.toString());
			// Load the Document and print it
			System.out.println(repo.findById(id));

			// Run sample N1QL query.
			String query = "SELECT name FROM ATT WHERE $1 IN interests";
			String parameter = "African Swallows";
			List<String> resultSet = repo.getQueryResultFor(query, parameter);
			System.err.println(resultSet.size());

			// Delete the document
			repo.delete(id, arthur.toString());

		} catch (Exception e) {
			LOGGER.error("Exception occurred while performing CRUD operation " + e.getMessage());
			// TODO : Add your exception logic here.
		} finally {
			// Free up resources after use.
			repo.terminateSession();
		}

	}
}