/**
 * 
 */
package com.zenika.batch.item.database.mongo;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * @author acogoluegnes
 *
 */
public class MongoDbCursorItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements InitializingBean {
	
	private Mongo mongo;
	
	private String databaseName;
	
	private MongoDbFactory mongoDbFactory;
	
	private DBCollection collection;
	
	private String collectionName;
	
	private DBCursor cursor;
	
	private String [] fields;
	
	private DbObjectMapper<T> dbObjectMapper;
	
	@Override
	protected void doOpen() throws Exception {
		cursor = collection.find(createDbObjectRef(),createDbObjectKeys());
	}

	@Override
	protected T doRead() throws Exception {
		if(!cursor.hasNext()) {
			return null;
		} else {
			DBObject dbObj = cursor.next();
			return dbObjectMapper.map(dbObj);
		}
	}
	
	@Override
	protected void doClose() throws Exception {
		cursor.close();
	}
	
	@Override
	protected void jumpToItem(int itemIndex) throws Exception {
		cursor = cursor.skip(itemIndex);
	}
	
	private DBObject createDbObjectKeys() {
		return new BasicDBObject();
	}

	private DBObject createDbObjectRef() {
		return new BasicDBObject();
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		DB db = null;
		if(mongoDbFactory == null) {
			Assert.notNull(mongo,"Mongo must be specified if MongoDbFactory is null");
			Assert.notNull(databaseName,"Mongo AND database must be set");
			db = mongo.getDB(databaseName);
		} else {
			db = mongoDbFactory.getDb();
		}
		Assert.notNull(collectionName,"collectionName must be set");
		collection = db.getCollection(collectionName);
	}

	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public void setDbObjectMapper(DbObjectMapper<T> dbObjectMapper) {
		this.dbObjectMapper = dbObjectMapper;
	}
	
}
