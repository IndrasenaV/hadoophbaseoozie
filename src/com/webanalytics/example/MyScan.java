package com.webanalytics.example;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class MyScan {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "mytable");

		Scan scan1 = new Scan();
//		scan1.addFamily(Bytes.toBytes("cf1"));
		scan1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col2"));
		scan1.setStartRow(Bytes.toBytes("123400"));
		scan1.setStopRow(Bytes.toBytes("123700"));
		
//		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
//				new SubstringComparator("value2"));
//		scan1.setFilter(filter);

		ResultScanner scanner1 = table.getScanner(scan1);
		for (Result res : scanner1) {
			// System.out.println(res);
			// System.out.println(res.value());

			for (KeyValue kv : res.raw()) {
				System.out.println("KV: " + kv + ", Value: "
						+ Bytes.toString(kv.getValue()));
			}
		}
		
		scanner1.close();
		table.close();
	}
}