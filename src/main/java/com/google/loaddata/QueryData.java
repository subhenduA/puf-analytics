package com.google.loaddata;

import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.FieldValue;

import java.util.Iterator;
import java.util.List;

//queries the sameple data loaded by me 
public class QueryData 
{
	public static void main( String[] args )
	{
		// Instantiates a client
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

		QueryRequest queryRequest =
	    QueryRequest
	        .newBuilder(
	            "SELECT "
	                + "AVG(age) AS avg_age "
	                + "FROM `my_new_dataset.my_new_table`;")
	        // Use standard SQL syntax for queries.
	        // See: https://cloud.google.com/bigquery/sql-reference/
	        .setUseLegacySql(false)
	        .build();
		QueryResponse response = bigquery.query(queryRequest);

		// parse the result 
		QueryResult result = response.getResult();

		while (result != null) {
			Iterator<List<FieldValue>> iter = result.iterateAll();
			while (iter.hasNext()) {
    			List<FieldValue> row = iter.next();
    			System.out.println(row.get(0));
    		}
    		result = result.getNextPage();
		}


	}

}