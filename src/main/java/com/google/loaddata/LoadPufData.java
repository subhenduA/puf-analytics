package com.google.loaddata;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class LoadPufData
{
	public static void main( String[] args )
	{
		//Parse the first line
		String firstLine = "";
		try {
			BufferedReader buffer = new BufferedReader(new FileReader("//Users//subhenduaich//data//Medicare_Provider_Util_Payment_PUF_CY2014.txt"));
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
		// load data 
	}
}