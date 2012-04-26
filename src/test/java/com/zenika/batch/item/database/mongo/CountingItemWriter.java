/**
 * 
 */
package com.zenika.batch.item.database.mongo;

import java.util.List;

import org.springframework.batch.item.ItemWriter;

import com.google.common.base.Function;

/**
 * @author acogoluegnes
 *
 */
public class CountingItemWriter<T> implements ItemWriter<T> {
	
	private int counter = 0;
	
	private final Function<T,Boolean> alwaysNoPoisonPillFunction = new Function<T,Boolean>() {
		public Boolean apply(T input) {
			return false;			
		}
	};
	
	private Function<T,Boolean> poisonPillFunction = alwaysNoPoisonPillFunction; 

	/* (non-Javadoc)
	 * @see org.springframework.batch.item.ItemWriter#write(java.util.List)
	 */
	@Override
	public void write(List<? extends T> items) throws Exception {
		for(T item : items) {
			if(poisonPillFunction.apply(item)) {
				throw new RuntimeException("poison pill detected for item: "+item);
			}
			counter++;
		}
	}

	public void reset() {
		counter = 0;
	}
	
	public int getCounter() {
		return counter;
	}
	
	public void setPoisonPillFunction(Function<T, Boolean> poisonPillFunction) {
		if(poisonPillFunction == null) {
			this.poisonPillFunction = alwaysNoPoisonPillFunction;
		} else {
			this.poisonPillFunction = poisonPillFunction;
		}
	}
	
}
