package com.webanalytics.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import com.webanalytics.hbase.model.AnalyticTableConstant;
import com.webanalytics.hbase.model.RawDataTable;

public class InitializeAnalyticTables {

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);


		if (admin.tableExists(RawDataTable.TABLE_NAME)) {
			System.out.printf("Deleting %s\n",
					Bytes.toString(RawDataTable.TABLE_NAME));
			if (admin.isTableEnabled(RawDataTable.TABLE_NAME))
				admin.disableTable(RawDataTable.TABLE_NAME);
			admin.deleteTable(RawDataTable.TABLE_NAME);
		}
		if (admin.tableExists(AnalyticTableConstant.DAILY_TABLE_NAME)) {
			System.out.printf("Deleting %s\n",
					Bytes.toString(AnalyticTableConstant.DAILY_TABLE_NAME));
			if (admin.isTableEnabled(AnalyticTableConstant.DAILY_TABLE_NAME))
				admin.disableTable(AnalyticTableConstant.DAILY_TABLE_NAME);
			admin.deleteTable(AnalyticTableConstant.DAILY_TABLE_NAME);
		}

		if (admin.tableExists(RawDataTable.TABLE_NAME)) {
			System.out.println("RAW Data table already exists.");
		} else {
			System.out.println("Creating Raw Data table...");
			HTableDescriptor desc = new HTableDescriptor(
					RawDataTable.TABLE_NAME);
			HColumnDescriptor c = new HColumnDescriptor(
					RawDataTable.COLUMN_FAMILY);
			desc.addFamily(c);
			admin.createTable(desc);
			System.out.println("Raw data table created.");
		}

		if (admin.tableExists(AnalyticTableConstant.DAILY_TABLE_NAME)) {
			System.out.println("Daily analytic table already exists ...");
		} else {
			System.out.println("Creating daily analytic table ...");
			HTableDescriptor desc = new HTableDescriptor(
					AnalyticTableConstant.DAILY_TABLE_NAME);
			HColumnDescriptor c = new HColumnDescriptor(
					AnalyticTableConstant.PAGEHIT_COLUMN_FAMILY);
			desc.addFamily(c);
			c = new HColumnDescriptor(AnalyticTableConstant.BROWSER_COLUMN_FAMILY);
			desc.addFamily(c);
			c = new HColumnDescriptor(AnalyticTableConstant.LOCATION_COLUMN_FAMILY);
			desc.addFamily(c);
			c = new HColumnDescriptor(AnalyticTableConstant.UNIQUE_VISIT);
			desc.addFamily(c);
			c = new HColumnDescriptor(AnalyticTableConstant.OS_COLUMN_FAMILY);
			desc.addFamily(c);
			c = new HColumnDescriptor(
					AnalyticTableConstant.SOCIAL_REFERRER_COLUMN_FAMILY);
			desc.addFamily(c);
			admin.createTable(desc);
			System.out.println("Daily Analytic Table created.");
		}

		admin.close();
	}
}
