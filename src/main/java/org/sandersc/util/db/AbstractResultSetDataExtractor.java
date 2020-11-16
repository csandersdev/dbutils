package org.sandersc.util.db;

import java.sql.ResultSet;

public class AbstractResultSetDataExtractor implements
		ResultSetDataExtractor {

	@Override
	public boolean preProcess(ResultSet resultSet) throws Exception {
		return true;
	}

	@Override
	public void postProcess(ResultSet rs, int rowCount) throws Exception {
		//do nothing
	}

	@Override
	public void processRow(ResultSet resultSet) throws Exception {
		// do nothing
	}

}
