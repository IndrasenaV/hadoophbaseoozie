package com.webanalytics.hbase.model;

import org.apache.hadoop.hbase.util.Bytes;

public class IntegrationResult {


	public static final byte[] TABLE_NAME = Bytes.toBytes("INTEGRATION_RESULT_DATA");
	public static final byte[] COLUMN_FAMILY = Bytes.toBytes("result");
	
	
}
