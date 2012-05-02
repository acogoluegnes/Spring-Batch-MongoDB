/**
 * 
 */
package com.zenika.batch.item.database.mongo;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
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
	
	private DBCollection collection;
	
	private String collectionName;
	
	private DBCursor cursor;
	
	private String [] fields;
	
	private DBObject refDbObject;
	
	private DbObjectMapper<T> dbObjectMapper;
	
	public MongoDbCursorItemReader() {
		super();
		setName(ClassUtils.getShortName(MongoDbCursorItemReader.class));
	}
	
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
	
	protected DBObject createDbObjectKeys() {
		if(fields == null) {
			return new BasicDBObject();
		} else {
			BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
			for(String field : fields) {
				builder.add(field,1);
			}
			return builder.get();
		}
	}

	protected DBObject createDbObjectRef() {
		if(refDbObject == null) {
			return new BasicDBObject();
		} else {
			return refDbObject;
		}
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(mongo,"Mongo must be specified if MongoDbFactory is null");
		Assert.notNull(databaseName,"Mongo AND database must be set");
		Assert.notNull(collectionName,"collectionName must be set");
		DB db = mongo.getDB(databaseName);
		collection = db.getCollection(collectionName);
	}
	
	public void setRefDbObject(DBObject refDbObject) {
		this.refDbObject = refDbObject;
	}

	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}
	
	public void setFields(String[] fields) {
		this.fields = fields;
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
