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

public class LoadPufData
{

	public static void main( String[] args ) throws Exception
	{
		
		//Parse the first line
		String firstLine = "";
		String DATASET_NAME = "my_new_dataset";
		String TABLE_NAME = "raw_puf_data";
		String FILE_PATH = "//Users//subhenduaich//data//Medicare_Provider_Util_Payment_PUF_CY2014.txt.sample";
		/**
		try {
			BufferedReader buffer = new BufferedReader(new FileReader(FILE_PATH));
			firstLine = buffer.readLine();
			buffer.close();
			System.out.println(firstLine);
		} catch (IOException ex) {
			// something bad happened, deal with it.
			ex.printStackTrace();
		}
		//Prepares the schema definition 
		ArrayList<Field>  fields_list = new ArrayList<Field>();
		for(String field: firstLine.split("\t")) {
			if(field.endsWith("_cnt") || field.endsWith("_amt")) 
				fields_list.add(Field.of(field, Field.Type.integer()));
			else
				fields_list.add(Field.of(field, Field.Type.string()));
		}
		//create table schema in bigquery dataset
		Schema schema = Schema.newBuilder().setFields(fields_list).build();
		//create table 
		TableId tableId = TableId.of(DATASET_NAME, TABLE_NAME);
		TableDefinition tableDefinition = StandardTableDefinition.of(schema);
		TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
		// Instantiates a client
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		Table table = bigquery.create(tableInfo);
		**/
		// load data
		System.out.println(writeFileToTable(DATASET_NAME, TABLE_NAME, FileSystems.getDefault().getPath(FILE_PATH)));
	}

	public static long writeFileToTable(String datasetName, String tableName, Path csvPath)
      throws IOException, InterruptedException, TimeoutException {
	    // [START writeFileToTable]
	    TableId tableId = TableId.of(datasetName, tableName);
	    CsvOptions csv_options = CsvOptions.builder().fieldDelimiter("\t").skipLeadingRows(2).build();
	    WriteChannelConfiguration writeChannelConfiguration =
	        WriteChannelConfiguration.newBuilder(tableId)
	            .setFormatOptions(csv_options)
	            .build();

	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    TableDataWriteChannel writer = bigquery.writer(writeChannelConfiguration);
	    
	    // Write data to writer
	    try {
	    	OutputStream stream = Channels.newOutputStream(writer);
	    	//Files.copy(csvPath, stream);
	    	System.out.println(Files.copy(csvPath, stream));
	    }catch(Exception e) {

	    	e.printStackTrace();
	    }
	    
	    // Get load job
	    Job job = writer.getJob();
	    job = job.waitFor();
	    LoadStatistics stats = job.getStatistics();
	    return stats.getOutputRows();
	    // [END writeFileToTable]
  }
}