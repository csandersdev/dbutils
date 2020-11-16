package org.sandersc.util.db;

import static org.junit.Assert.*;

import java.sql.Connection;

import org.junit.Test;

public class ConnectionManagerTest {

	@Test(expected=java.lang.IllegalArgumentException.class)
	public void testNamedDatasourceConnectionNotExists() throws Exception {
		Connection conn = ConnectionManager.getConnection("NOT_EXISTS");
		if( conn != null ) { 
			conn.close();
		}
		fail("Obtained a connection for invalid datasource name");
	}
	
	@Test
	public void testDefaultDatasourceConnection() throws Exception {
		Connection conn = ConnectionManager.getConnection();
		try {
			assertNotNull(conn);
			assertFalse(conn.isClosed());
		} finally {
			conn.close();
		}		
	}
	
	@Test
	public void testNamedDatasourceConnectionExists() throws Exception {
		String datasource = "NEWDS";
		
		ConnectionManager.addDatasource(datasource, "db_config.properties");
		Connection conn = ConnectionManager.getConnection(datasource);
		try {
			assertNotNull(conn);
			assertFalse(conn.isClosed());
		} finally {
			conn.close();
		}
	}
}
