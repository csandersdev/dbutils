package org.sandersc.util.db;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class JdbcTemplateTest {

	
	@Test
	@SuppressWarnings("rawtypes")
	public void testSuccessQueryForListNone() throws Exception {

		List list = null;
		
		Connection conn = ConnectionManager.getConnection();
		try {
			JdbcTemplate jdbcTemplate = new JdbcTemplate();
			jdbcTemplate.setConnection(conn);
			list = jdbcTemplate.queryForList("select tablename from sys.systables where 1=0");
		} finally {
			conn.close();
		}
		
		assertNotNull(list);
		assertEquals(list.size(), 0);
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testSuccessQueryForListRows() throws Exception {
		List list = null;
		
		Connection conn = ConnectionManager.getConnection();
		try {
			JdbcTemplate jdbcTemplate = new JdbcTemplate();
			jdbcTemplate.setConnection(conn);
			list = jdbcTemplate.queryForList("select tablename from sys.systables where tablename in ('SYSTABLES', 'SYSKEYS') order by tablename");
		} finally {
			conn.close();
		}
		
		assertNotNull(list);
		assertEquals(2, list.size());
		
		Map row = (Map) list.get(0);
		assertEquals(1, row.size());
		assertEquals(row.get("TABLENAME"), "SYSKEYS");
		
	}	
}
