/**
 * 
 */
package com.zenika.batch.item.database.mongo;

import org.springframework.core.convert.converter.Converter;

import com.mongodb.DBObject;

/**
 * @author acogoluegnes
 *
 */
public class PassthroughDbObjectConverter implements Converter<DBObject,DBObject> {

	@Override
	public DBObject convert(DBObject source) {
		return source;
	}

}
