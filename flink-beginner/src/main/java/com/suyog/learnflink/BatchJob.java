package com.suyog.learnflink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class BatchJob {

	private static final Logger logger = LoggerFactory.getLogger(BatchJob.class.getName());
	public static void main(String[] args) throws Exception {
		try {
			// set up the batch execution environment
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			List<String> products = Arrays.asList("Mouse", "Keyboard", "Monitor");

			DataSet<String> dsProducts = env.fromCollection(products);
			logger.info("\n Total products count = " + dsProducts.count());
			// execute program
			env.execute("Flink Batch Java API Skeleton");
		} catch (Exception e){

		}
	}
}
