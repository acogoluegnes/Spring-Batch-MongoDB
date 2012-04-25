/**
 * 
 */
package com.zenika.batch.item.database.mongo;

import com.mongodb.DBObject;

/**
 * @author acogoluegnes
 *
 */
public interface DbObjectMapper<T> {

	T map(DBObject dbObject);
	
}
