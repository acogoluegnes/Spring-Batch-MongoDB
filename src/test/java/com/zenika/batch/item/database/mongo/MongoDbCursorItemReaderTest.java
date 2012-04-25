/**
 * 
 */
package com.zenika.batch.item.database.mongo;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.util.ExecutionContextUserSupport;

import com.google.common.collect.ImmutableMap;
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
	
	private String readerName = "mongo-reader";
	
	private Mongo mongo;
	
	@Before public void setUp() throws Exception {
		mongo = new Mongo();
		initReader();
		collection().drop();
	}

	private void initReader() throws Exception {
		reader = new MongoDbCursorItemReader<DBObject>();
		reader.setMongo(mongo);
		reader.setDatabaseName(databaseName);
		reader.setCollectionName("dummy");
		reader.setName(readerName);
		reader.setDbObjectMapper(new PassthroughDbObjectMapper());
		reader.afterPropertiesSet();
	}

	@Test public void justRead() throws Exception {
		int docCount = 20;
		insertDocuments(docCount);
		reader.open(new ExecutionContext());
		int itemCount = 0;
		DBObject doc = null;
		while((doc = reader.read()) != null) {
			itemCount++;
			assertThat(doc.containsField("number"),is(true));
			assertThat(doc.containsField("name"),is(true));
		}
		assertThat(itemCount,is(docCount));
		reader.close();
	}

	@Test public void readWithRefObject() throws Exception {
		int docCount = 20;
		insertDocuments(docCount);
		int limit = 12;
		reader.setRefMap(ImmutableMap.of("number",ImmutableMap.of("$gt",limit)));
		reader.open(new ExecutionContext());
		int itemCount = 0;
		while(reader.read() != null) {
			itemCount++;
		}
		assertThat(itemCount,is(docCount-limit-1));
		reader.close();
	}
	
	@Test public void readWithFields() throws Exception {
		int docCount = 20;
		insertDocuments(docCount);
		reader.setFields(new String[]{"number"});
		reader.open(new ExecutionContext());
		int itemCount = 0;
		DBObject doc = null;
		while((doc = reader.read()) != null) {
			itemCount++;
			assertThat(doc.containsField("number"),is(true));
			assertThat(doc.containsField("name"),is(false));
		}
		assertThat(itemCount,is(docCount));
		reader.close();
	}
	
	@Test public void readRestart() throws Exception {
		int docCount = 20;
		insertDocuments(docCount);
		int offset = 12;
		reader.open(executionContext(offset));
		int itemCount = 0;
		DBObject doc = null;
		while((doc = reader.read()) != null) {
			itemCount++;
			assertThat(((Number) doc.get("number")).intValue(),greaterThanOrEqualTo(offset));
		}
		assertThat(itemCount,is(docCount-offset));
		reader.close();
	}
	
	private ExecutionContext executionContext(int readCount) {
		ExecutionContext ctx = new ExecutionContext();
		ctx.putInt(new ExecutionContextUserSupport(readerName).getKey("read.count"),readCount);
		return ctx;
	}

	private void insertDocuments(int docCount) {
		for(int i=0;i<docCount;i++) {
			collection().insert(constructDbo(i));
		}
	}
	
	private DBObject constructDbo(int i) {
		return BasicDBObjectBuilder.start()
				.add("number",i)
				.add("name","bla "+i)
				.get();
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
