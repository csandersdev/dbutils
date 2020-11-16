package org.sandersc.util.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unchecked", "rawtypes"})
public class MapListResultSetExtractor implements ResultSetDataExtractor {

	private List<Map> result = null;
	
	@Override
	public boolean preProcess(ResultSet resultSet) throws Exception {
		result = new ArrayList<Map>();
		
		return true;
	}

	
	@Override
	public void processRow(ResultSet resultSet) throws Exception {
		
		Map row = new HashMap();
		
		ResultSetMetaData rsmd = resultSet.getMetaData();
		for( int col=1; col <= rsmd.getColumnCount(); col++ ) {			
			
			if( rsmd.getColumnType(col) == Types.CLOB || rsmd.getColumnType(col) == Types.BLOB ) { 
				row.put(rsmd.getColumnLabel(col), String.format("<LOB-%d>", rsmd.getColumnType(col)));
			} else {
				String result = resultSet.getString(col);
				row.put(rsmd.getColumnLabel(col), result);
			}
		}
		
		getResult().add(row);
	}

	@Override
	public void postProcess(ResultSet rs, int rowCount) throws Exception {
		// do nothing
	}


	public List<Map> getResult() {
		return result;
	}
}
