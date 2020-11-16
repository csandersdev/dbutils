package org.sandersc.util.db;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConnectionManager {

	public static class ConnectionProperties {
		public ConnectionProperties() {

		}

		public ConnectionProperties(Properties p) throws Exception {
			Enumeration<?> en = p.propertyNames();
			while( en.hasMoreElements() ) {
				String propertyName = (String) en.nextElement();
				String propertyValue = p.getProperty(propertyName);

				if( "dbDriver".equals(propertyName) ) {
					this.driver = propertyValue;
				} else if( "dbUrl".equals(propertyName) ) {
					this.url = propertyValue;
				} else if ("dbUsername".equals(propertyName) ) {
					this.username = propertyValue;
					this.properties.put("user", this.username);
				} else if( "dbPassword".equals(propertyName) ) {
					this.password = propertyValue;
					this.properties.put("password", this.password);
				} else {
					this.properties.put(propertyName, propertyValue);
				}
			}
		}

		public String driver;
		public String url;
		public String username;
		public String password;
		public Properties properties = new Properties();
	}

	public static final String DB_CONFIG_PATH = "org.sandersc.db.config";
	public static final String DEFAULT_DB_CONFIG_PATH = "db_config.properties";

	private static final String DEFAULT_DATASOURCE_NAME = "__default__";
	private static final Map<String,ConnectionProperties> datasources = new HashMap<String,ConnectionProperties>();

	private ConnectionManager() {
	}

	public static synchronized Connection getConnection() throws Exception {
		if (datasources.get(DEFAULT_DATASOURCE_NAME) == null) {
			addDatasource(DEFAULT_DATASOURCE_NAME, getPropertyFilePath());
		}

		return getConnection(DEFAULT_DATASOURCE_NAME);
	}

	public static synchronized Connection getConnection(String datasource) throws Exception {

		ConnectionProperties connProps = datasources.get(datasource);
		if( connProps == null ) {
			throw new IllegalArgumentException(String.format("No datasource configured for datasource name %s", datasource));
		}

		Connection conn =  DriverManager.getConnection(connProps.url, connProps.properties);

		return conn;
	}

	private static String getPropertyFilePath() {
		String propertyFilePath = System.getProperty(DB_CONFIG_PATH);
		if (propertyFilePath == null) {
			propertyFilePath = DEFAULT_DB_CONFIG_PATH;
		}
		return propertyFilePath;
	}

	public static void addDatasource(String datasource, String propertyFilePath) throws Exception {
		Properties p = new Properties();
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyFilePath);
		if( is == null ) {
			is = new FileInputStream(propertyFilePath);
		}

		try {
			p.load(is);
		} finally {
			is.close();
		}

		ConnectionProperties connProps = new ConnectionProperties(p);

		// Make sure the driver is loaded
		Class.forName(connProps.driver);

		datasources.put(datasource, connProps);
	}
}
