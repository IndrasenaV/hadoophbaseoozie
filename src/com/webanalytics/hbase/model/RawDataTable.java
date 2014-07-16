package com.webanalytics.hbase.model;

import org.apache.hadoop.hbase.util.Bytes;

public class RawDataTable {

	public static final byte[] TABLE_NAME = Bytes.toBytes("RawDataTable");
	public static final byte[] COLUMN_FAMILY = Bytes.toBytes("XML_DATA");
	
	private String primaryKey;
	private String columnFamilyName;
	private String columnIndex;
	private String columnIndexValue;
	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	public String getColumnFamilyName() {
		return columnFamilyName;
	}
	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}
	public String getColumnIndex() {
		return columnIndex;
	}
	public void setColumnIndex(String columnIndex) {
		this.columnIndex = columnIndex;
	}
	public String getColumnIndexValue() {
		return columnIndexValue;
	}
	public void setColumnIndexValue(String columnIndexValue) {
		this.columnIndexValue = columnIndexValue;
	}
	
}
