package org.sandersc.util.db;

import java.sql.ResultSet;

public class ScalarResultSetExtractor implements ResultSetDataExtractor {

	private Object result = null;
	
	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}

	@Override
	public boolean preProcess(ResultSet resultSet) throws Exception {
		return true;
	}

	@Override
	public void processRow(ResultSet resultSet) throws Exception {
		if( result == null ) { 
			result = resultSet.getObject(1);
		} else {
			throw new IllegalArgumentException("Incorrect result size - more than one result");
		}
	}

	@Override
	public void postProcess(ResultSet rs, int rowCount) throws Exception {
		if( result == null ) {
			throw new IllegalArgumentException("Incorrect result size - no results found");
		}
	}

}
