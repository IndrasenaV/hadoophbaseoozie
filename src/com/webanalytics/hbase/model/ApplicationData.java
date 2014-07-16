package com.webanalytics.hbase.model;

import org.apache.hadoop.hbase.util.Bytes;

public class ApplicationData {

	private static final byte[] TABLE_NAME = Bytes
			.toBytes("ApplicationDataTable");
}
