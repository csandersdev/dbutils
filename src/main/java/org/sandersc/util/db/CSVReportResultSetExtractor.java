package org.sandersc.util.db;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import org.apache.commons.io.IOUtils;

public class CSVReportResultSetExtractor implements ResultSetDataExtractor {

	public static final String DEFAULT_FIELD_SEP = ",";

	private static final String DEFAULT_BLOB_DIR_PATH = ".";
	private static final boolean DEFAULT_FORCE_OVERWRITE = true;
	private static final boolean DEFAULT_PRINT_HEADER = true;
	
	private PrintStream printStream;
	
	private String dataDirPath = DEFAULT_BLOB_DIR_PATH;
	private boolean overwrite = DEFAULT_FORCE_OVERWRITE;
	private boolean printHeader = DEFAULT_PRINT_HEADER;
	private String fieldSep = DEFAULT_FIELD_SEP;
//	private boolean quoteFields = false;
//	private String quoteString = "\"";

	private Map<String, String> lobNameColumnMap;

	public CSVReportResultSetExtractor(PrintStream ps) {
		this.printStream = ps;	
	}
	
	public CSVReportResultSetExtractor(PrintStream ps, String dataDirPath, boolean overwrite, boolean printHeader) {
		this.printStream = ps;	
		setDataDirPath(dataDirPath);
		setOverwrite(overwrite);
		setPrintHeader(printHeader);
	}	
	
	public String getDataDirPath() {
		return dataDirPath;
	}

	public void setDataDirPath(String dataDirPath) {
		this.dataDirPath = dataDirPath;
	}
	
	public boolean isOverwrite() {
		return this.overwrite;
	}

	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}
	
	public boolean isPrintHeader() {
		return this.printHeader;
	}

	public void setPrintHeader(boolean printHeader) {
		this.printHeader = printHeader;
	}
	
	public String getFieldSeparator(String fieldSep) {
		return fieldSep;
	}	
	
	public void setFieldSeparator(String fieldSep) {
		this.fieldSep = fieldSep;
	}
	
	public Map<String, String> getLobNameColumnMap() {
		return lobNameColumnMap;
	}
	
	public void setLobNameColumnMap(Map<String, String> lobNameColumnMap) {
		this.lobNameColumnMap = lobNameColumnMap;
	}	

	@Override
	public boolean preProcess(ResultSet resultSet) throws Exception {

		if( isPrintHeader() ) {
			ResultSetMetaData rsmd = resultSet.getMetaData();
			for( int col=1; col <= rsmd.getColumnCount(); col++ ) {
				if( col > 1 ) {
					printStream.print(fieldSep);
				}
				String colName = rsmd.getColumnLabel(col);
				printStream.print(colName);
			}
			
			printStream.println("");
		}
		
		return true;
	}
	
	@Override
	public void processRow(ResultSet resultSet) throws Exception {
		ResultSetMetaData rsmd = resultSet.getMetaData();
		for( int col=1; col <= rsmd.getColumnCount(); col++ ) {
			
			if( col > 1 ) {
				printStream.print(fieldSep);
			}			
			
			if( rsmd.getColumnType(col) == Types.CLOB ) {
			
				try( Reader reader = resultSet.getCharacterStream(col) ) {
					if( reader != null ) {
						File file = getOutputFile(resultSet, col);
						if( isOverwrite() || ! file.exists() ) {
							try( Writer writer = new FileWriter(file) ) {
								IOUtils.copy(reader, writer);
							}
						}
						printStream.print(String.format("<%s>", file.getName()));
					}
				}
				
			} else if( rsmd.getColumnType(col) == Types.BLOB ) { 
				
				try( InputStream is = resultSet.getBinaryStream(col) ) {
					if( is != null ) {  
						File file = getOutputFile(resultSet, col);
						if( isOverwrite()  || ! file.exists() ) {
							try( OutputStream os = new FileOutputStream(file) ) {
								IOUtils.copy(is, os);
							}
						}
						printStream.print(String.format("<%s>", file.getName()));
					}
				}
				
			} else {
				String result = resultSet.getString(col);
				printStream.print(result);
			}
		}
		
		printStream.println("");
	}
	
	protected File getOutputFile(ResultSet rs, int column) throws SQLException {

		ResultSetMetaData rsmd = rs.getMetaData();
		
		String outputFileName = null;
			
		String columnName = rsmd.getColumnName(column);
		String nameColumnName = lobNameColumnMap.get( columnName );
		if( nameColumnName != null ) {
			outputFileName = rs.getString(nameColumnName);
		} else {
			outputFileName = getDefaultFileName(rs, column, rsmd);
		}
		
		String outputDirName = getDataDirPath();
		
		return new File(outputDirName, outputFileName);
	}

	private String getDefaultFileName(ResultSet rs, int column,
			ResultSetMetaData rsmd) throws SQLException {
		String prefix = getPrefixForColumnType(rsmd, column);
		String uniqueName = getUniqueNameForColumn(rs, column);
		String suffix = getSuffixForColumnType(rsmd, column);
		String outputFileName = String.format("%s-%s.%s", prefix, uniqueName, suffix);
		return outputFileName;
	}

	protected String getPrefixForColumnType(ResultSetMetaData rsmd, int column) throws SQLException {
		String prefix = "lob";
		
		if( rsmd.getColumnType(column) == Types.CLOB ) {
			prefix = "clob";
		} else if( rsmd.getColumnType(column) == Types.BLOB ) {
			prefix = "blob";
		}
		return prefix;
	}
	
	private String getUniqueNameForColumn(ResultSet rs, int column) {
		return String.valueOf(System.currentTimeMillis());
	}	
	
	protected String getSuffixForColumnType(ResultSetMetaData rsmd, int column) throws SQLException {
		String suffix = "lob";
		
		if( rsmd.getColumnType(column) == Types.CLOB ) {
			suffix = "txt";
		} else if( rsmd.getColumnType(column) == Types.BLOB ) {
			suffix = "dat";
		}
		return suffix;		
	}
	
	@Override
	public void postProcess(ResultSet rs, int rowCount) throws Exception {
		if( rowCount == 0 ) {
			printStream.println("No results found.");
		}
	}
}
