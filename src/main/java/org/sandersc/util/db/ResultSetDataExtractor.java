package org.sandersc.util.db;

import java.sql.ResultSet;

/**
 * Definition of a row processing interface for applications to plugin-specific
 * logic for dealing with the results of various SQL statements.
 */
public interface ResultSetDataExtractor {

	boolean preProcess(ResultSet resultSet) throws Exception;
	
	public void processRow(ResultSet resultSet) throws Exception;

	void postProcess(ResultSet rs, int rowCount) throws Exception;
}
