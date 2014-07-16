package com.webanalytics.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.tomcat.dbcp.dbcp.BasicDataSource;

public class IpInfoLoad {

	Map<String, Integer> locationCache = new HashMap<String, Integer>();
	Connection localConn = null;
	Connection remoteConn = null;

	protected void setup() {
		BasicDataSource remoteDs = new BasicDataSource();
		remoteDs.setDriverClassName("com.mysql.jdbc.Driver");
		remoteDs.setUsername("pgrwrite");
		remoteDs.setPassword("Sravya0828");
		remoteDs.setUrl("jdbc:mysql://pgrwrite.db.7322579.hostedresource.com/pgrwrite");
		remoteDs.setMaxActive(10);
		remoteDs.setMaxIdle(5);
		remoteDs.setInitialSize(5);
		try {
			remoteConn = remoteDs.getConnection();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		BasicDataSource localDs = new BasicDataSource();
		localDs.setDriverClassName("com.mysql.jdbc.Driver");
		localDs.setUsername("root");
		localDs.setPassword("");
		localDs.setUrl("jdbc:mysql://localhost:3306/test");
		localDs.setMaxActive(10);
		localDs.setMaxIdle(5);
		localDs.setInitialSize(5);
		try {
			localConn = localDs.getConnection();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void cleanup() {
		try {
			remoteConn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			localConn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Integer getFromDb() {
		try {
			Statement remoteStat = remoteConn.createStatement();
			Statement localStat = localConn.createStatement();
			for (int i = 0; i < 256; i++) {
				System.out.println("Loading table " + i + " started ");
				for (int j = 0; j < 256; j++) {
					ResultSet remoteRs = remoteStat
							.executeQuery("select c,city from ip4_" + i
									+ " where b = " + j);
					while (remoteRs.next()) {
						localStat
								.executeUpdate("insert into ipAddress Values ( "
										+ i
										+ ","
										+ j
										+ ","
										+ remoteRs.getInt(1)
										+ ","
										+ remoteRs.getInt(2) + ")");
					}
					remoteRs.close();
				}
				System.out.println("Loading table " + i + " done");
			}
			localStat.close();
			remoteStat.close();
			return null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private Integer loadCities() {
		try {
			Statement remoteStat = remoteConn.createStatement();
			Statement localStat = localConn.createStatement();

			System.out.println("started table ");
			ResultSet remoteRs = remoteStat
					.executeQuery("SELECT city,country,name,lat,lng,state FROM cityByCountry where country = 226");
			while (remoteRs.next()) {
				localStat
						.executeUpdate("INSERT INTO cityByCountry (city,country,name,lat,lng,state) VALUES ('"
								+ remoteRs.getString(1)
								+ "' ,'"
								+ remoteRs.getString(2)
								+ "','"
								+ remoteRs.getString(3)
								+ "',"
								+ remoteRs.getDouble(4)
								+ ","
								+ remoteRs.getDouble(5)
								+ ",'"
								+ remoteRs.getString(6) + "')");
			}
			remoteRs.close();

			localStat.close();
			remoteStat.close();
			return null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		IpInfoLoad ll = new IpInfoLoad();
		ll.setup();
		// ll.getFromDb();
		ll.loadCities();
		ll.cleanup();
	}
}
