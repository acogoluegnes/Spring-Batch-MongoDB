/**
 * 
 */
package com.zenika.batch.item.database.mongo;

import com.mongodb.DBObject;

/**
 * @author acogoluegnes
 *
 */
public class PassthroughDbObjectMapper implements DbObjectMapper<DBObject> {

	/* (non-Javadoc)
	 * @see com.zenika.batch.item.database.mongo.DbObjectMapper#map(com.mongodb.DBObject)
	 */
	@Override
	public DBObject map(DBObject dbObject) {
		return dbObject;
	}

}
