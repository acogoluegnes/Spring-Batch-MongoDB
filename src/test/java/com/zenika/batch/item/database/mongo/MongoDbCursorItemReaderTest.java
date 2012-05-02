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
import org.springframework.core.io.support.PropertiesLoaderUtils;

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
	
	private String databaseName;
	
	private String collectionName = "dummy";
	
	private String readerName = "mongo-reader";
	
	private Mongo mongo;
	
	@Before public void setUp() throws Exception {
		databaseName = PropertiesLoaderUtils.loadAllProperties("mongo.properties").getProperty("mongo.db");
		mongo = new Mongo();
		reader = initReader(DBObject.class);
		reader.setDbObjectMapper(new PassthroughDbObjectMapper());
		collection().drop();
	}

	private <T> MongoDbCursorItemReader<T> initReader(Class<T> cl) throws Exception {
		MongoDbCursorItemReader<T> r = new MongoDbCursorItemReader<T>();
		r.setMongo(mongo);
		r.setDatabaseName(databaseName);
		r.setCollectionName("dummy");
		r.setName(readerName);		
		r.afterPropertiesSet();
		return r;
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
		reader.setRefDbObject(BasicDBObjectBuilder.start().push("number").add("$gt",limit).pop().get());
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
	
	@Test public void mapDoc() throws Exception {
		int docCount = 20;
		insertDocuments(docCount);
		MongoDbCursorItemReader<Dummy> r = initReader(Dummy.class);
		r.setDbObjectMapper(new DummyObjectMapper());
		r.open(new ExecutionContext());
		int itemCount = 0;
		Dummy doc = null;
		while((doc = r.read()) != null) {
			assertThat(doc.number,is(itemCount));
			assertThat(doc.name,is("bla "+itemCount));
			itemCount++;			
		}
		assertThat(itemCount,is(docCount));
		r.close();
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
	
	private static class Dummy {
		
		private final Integer number;
		private final String name;
		
		public Dummy(Integer number, String name) {
			super();
			this.number = number;
			this.name = name;
		}
		
	}
	
	private static class DummyObjectMapper implements DbObjectMapper<Dummy> {
		@Override
		public Dummy map(DBObject dbObject) {
			return new Dummy(((Number) dbObject.get("number")).intValue(),dbObject.get("name").toString());
		}
	}
	
}
