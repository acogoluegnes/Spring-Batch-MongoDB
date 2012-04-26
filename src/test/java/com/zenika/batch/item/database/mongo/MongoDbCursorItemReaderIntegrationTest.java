/**
 * 
 */
package com.zenika.batch.item.database.mongo;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.base.Function;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * @author acogoluegnes
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoDbCursorItemReaderIntegrationTest {

	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	Job job;

	@Autowired
	CountingItemWriter<DBObject> writer;

	@Autowired
	Mongo mongo;

	@Value("${mongo.db}")
	String databaseName;

	private String collectionName = "dummy";

	@Before
	public void setUp() throws Exception {
		writer.reset();
		collection().drop();
	}

	@Test
	public void simpleReading() throws Exception {
		int docCount = 112;
		insertDocuments(docCount);

		JobExecution exec = jobLauncher.run(job, new JobParametersBuilder()
				.addLong("time", System.currentTimeMillis()).toJobParameters());
		assertThat(exec.getStatus(), is(BatchStatus.COMPLETED));
		assertThat(writer.getCounter(), is(docCount));
	}

	@Test public void restartAfterFailure() throws Exception {
		int docCount = 112;
		insertDocuments(docCount);
		
		final Integer poison = new Integer(53);
		writer.setPoisonPillFunction(new Function<DBObject,Boolean>() {
			@Override
			public Boolean apply(DBObject input) {
				return input.get("number").equals(poison);
			}
		});
		
		JobExecution exec = jobLauncher.run(
			job,
			new JobParametersBuilder().addLong("time",System.currentTimeMillis()).toJobParameters()
		);
		assertThat(exec.getStatus(),is(BatchStatus.FAILED));
		assertThat(writer.getCounter(),is(poison));
		
		writer.setPoisonPillFunction(null);
		
		exec = jobLauncher.run(
			job,
			exec.getJobInstance().getJobParameters()
		);
		int chunkSize = 5;
		int itemsThatWillComeBackAgain = poison % chunkSize; 
		assertThat(exec.getStatus(),is(BatchStatus.COMPLETED));
		assertThat(writer.getCounter(),is(docCount+itemsThatWillComeBackAgain));
	}

	private void insertDocuments(int docCount) {
		for (int i = 0; i < docCount; i++) {
			collection().insert(constructDbo(i));
		}
	}

	private DBObject constructDbo(int i) {
		return BasicDBObjectBuilder.start().add("number", i)
				.add("name", "bla " + i).get();
	}

	private DBCollection collection() {
		return mongo.getDB(databaseName).getCollection(collectionName);
	}

}
