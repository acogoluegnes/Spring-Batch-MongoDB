/**
 * 
 */
package com.zenika.batch.item.database.mongo;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * @author acogoluegnes
 *
 */
public class MongoDbCursorItemReaderTest {
	
	private MongoDbCursorItemReader<DBObject> reader;
	
	private String databaseName = "spring-batch-mongo-test";
	
	private String collectionName = "dummy";
	
	private Mongo mongo;
	
	@Before public void setUp() throws Exception {
		mongo = new Mongo();
		reader = new MongoDbCursorItemReader<DBObject>();
		reader.setMongo(mongo);
		reader.setDatabaseName(databaseName);
		reader.setCollectionName("dummy");
		reader.setName("mongo-reader");
		reader.setDbObjectMapper(new PassthroughDbObjectMapper());
		reader.afterPropertiesSet();
		collection().drop();
	}

	@Test public void justRead() throws Exception {
		int rowCount = 20;
		for(int i=0;i<rowCount;i++) {
			collection().insert(BasicDBObjectBuilder.start().add("number",i).get());
		}
		reader.open(new ExecutionContext());
		int itemCount = 0;
		DBObject doc;
		while((doc = reader.read()) != null) {
			itemCount++;
		}
		assertThat(itemCount,is(rowCount));
		reader.close();
	}
	
	private DBCollection collection() {
		return mongo.getDB(databaseName).getCollection(collectionName); 
	}
	
	private static class PassthroughDbObjectMapper implements DbObjectMapper<DBObject> {

		@Override
		public DBObject map(DBObject dbObject) {
			return dbObject;
		}
		
	}
	
}
