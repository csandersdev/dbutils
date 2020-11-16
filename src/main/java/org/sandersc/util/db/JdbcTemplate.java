package org.sandersc.util.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class JdbcTemplate {
	
	private Connection conn = null;
	
	public JdbcTemplate() {
	}

	public void setConnection(Connection conn) {
		this.conn = conn;		
	}

	public void query(String sql, 
			ResultSetDataExtractor dataExtractor) throws Exception {
		query(sql, null, dataExtractor);
	}
	
	public void query(String sql, Object[] args,
			ResultSetDataExtractor dataExtractor) throws Exception {
		PreparedStatement statement = conn.prepareStatement(sql);
		try {
			setStatementParameters(statement, args);
			
			ResultSet resultSet = statement.executeQuery();
			try {
				boolean process = dataExtractor.preProcess(resultSet);
				
				if( process ) { 
					int rowCount = 0;
					for ( ; resultSet.next(); rowCount++ ) {
						dataExtractor.processRow(resultSet);
					}
					
					dataExtractor.postProcess(resultSet, rowCount);
				}
			} finally {
				resultSet.close();
			}
		} catch( SQLException sqle ) {
			System.err.println("Error executing SQL \"" + sql + "\"");
			throw sqle;
		} finally {
			statement.close();
		}
	}
	
	public int update(String sql) throws SQLException {
		return update(sql, null);
	}
	
	public int update(String sql, Object[] args) throws SQLException {
		int rowsAffected = 0;
		
		PreparedStatement ps = conn.prepareStatement(sql);
		try { 
			setStatementParameters(ps, args);
			
			rowsAffected = ps.executeUpdate();
		} finally {
			ps.close();
		}
		
		return rowsAffected;
	}	
	
	private void setStatementParameters(PreparedStatement statement,
			Object[] args) throws SQLException {
		if( args != null  ) {
			for( int i=0; i<args.length; i++) {
				statement.setString(i+1, args[i].toString());
			}
		}
	}	
	
	@SuppressWarnings("rawtypes")
	public List queryForList(String sql) throws Exception {
		return queryForList(sql, null);
	}
	
	@SuppressWarnings("rawtypes")
	public List queryForList(String sql, Object [] args) throws Exception {
	
		MapListResultSetExtractor extractor = new MapListResultSetExtractor();
		query(sql, args, extractor);
		
		return extractor.getResult();
	}

	public int queryForInt(String sql) throws Exception {
		return queryForInt(sql, null);
	}	
	
	public int queryForInt(String sql, Object [] args ) throws Exception {
		
		ScalarResultSetExtractor extractor = new ScalarResultSetExtractor();
		query(sql, args, extractor);
		
		return (Integer) extractor.getResult();
	}
	
	public long queryForLong(String sql) throws Exception {
		return queryForLong(sql, null);
	}	
	
	public long queryForLong(String sql, Object [] args ) throws Exception {
		
		ScalarResultSetExtractor extractor = new ScalarResultSetExtractor();
		query(sql, args, extractor);
		
		return (Long) extractor.getResult();
	}
	
	public boolean hasTable(String schemaName, String tableName) throws Exception { 
		boolean hasTable = false;
		
		ResultSet rs = conn.getMetaData().getTables(/*CATALOG=*/null, schemaName, tableName, null/*ALL TYPES*/);
		try { 
			hasTable = rs.next();
		} finally { 
			rs.close();
		}
		
		return hasTable;
	}
	
	public boolean hasColumn(String schemaName, String tableName, String columnName) throws Exception { 
		boolean hasColumn = false;
		
		ResultSet rs = conn.getMetaData().getColumns(/*CATALOG=*/null, schemaName, tableName, columnName);
		try { 
			hasColumn = rs.next();
		} finally { 
			rs.close();
		}
		
		return hasColumn;
	}
}
