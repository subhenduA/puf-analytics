package com.google.loaddata;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.nio.file.Files;
import java.util.concurrent.TimeoutException;
import java.nio.file.Path;
import java.nio.channels.Channels;
import java.nio.file.FileSystems;
import java.util.Date;
public class LoadPufData
{

	public static void main( String[] args ) throws Exception
	{
		
		
		String firstLine = "";
		String DATASET_NAME = "my_new_dataset";
		String TABLE_NAME = "raw_puf_data";
		String HEADER_FILE_PATH = "//Users//saich//data//header.txt";		
		String DATA_FILE_PATH = "//Users//saich//data//Medicare_Provider_Util_Payment_PUF_CY2014.txt.gz";
		
		//Parse the header file 
		try {
			BufferedReader buffer = new BufferedReader(new FileReader(HEADER_FILE_PATH));
			firstLine = buffer.readLine();
			buffer.close();
			System.out.println(firstLine);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		//Prepares a bigquery table schema from the header data 
		ArrayList<Field>  fields_list = new ArrayList<Field>();
		for(String field: firstLine.split("\t")) {
			if(field.endsWith("_cnt") || field.endsWith("_amt"))
				fields_list.add(Field.of(field, Field.Type.floatingPoint()));
			else 
				fields_list.add(Field.of(field, Field.Type.string()));
		}
		//create table schema in bigquery dataset
		Schema schema = Schema.newBuilder().setFields(fields_list).build();
		TableId tableId = TableId.of(DATASET_NAME, TABLE_NAME);
		TableDefinition tableDefinition = StandardTableDefinition.of(schema);
		TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		Table table = bigquery.create(tableInfo);
		
		// loads data in the new table 
		System.out.println(writeFileToTable(DATASET_NAME, TABLE_NAME, FileSystems.getDefault().getPath(DATA_FILE_PATH)));
	}

	public static int writeFileToTable(String datasetName, String tableName, Path csvPath)
      throws IOException, InterruptedException, TimeoutException {
	    // [START writeFileToTable]
	    TableId tableId = TableId.of(datasetName, tableName);
	    CsvOptions csv_options = CsvOptions.builder().fieldDelimiter("\t").skipLeadingRows(2).build();
	    WriteChannelConfiguration writeChannelConfiguration =
	        WriteChannelConfiguration.newBuilder(tableId)
	            .setFormatOptions(csv_options)
	            .build();
	        System.out.println(writeChannelConfiguration);

	    
	    try {
	    	BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		    TableDataWriteChannel writer = bigquery.writer(writeChannelConfiguration);
		    System.out.println(writer);
	    	// Write data to writer
	    	OutputStream stream = Channels.newOutputStream(writer);
	    	long start = System.currentTimeMillis();
	    	System.out.println("Bytes copied: " + Files.copy(csvPath, stream));
	    	stream.close();
	    	writer.close();
	    	long stop = System.currentTimeMillis();
	    	System.out.println("Copy Time taken: " + (stop -start)/1000 + "s");
	    	// access the load job
		    Job job = writer.getJob();
		    System.out.println(job);
		    job = job.waitFor();
		    System.out.println(job);
		    return 1;
	    }catch(Exception e) {
	    	e.printStackTrace();
	    	return -1;
	    } 
  }
}