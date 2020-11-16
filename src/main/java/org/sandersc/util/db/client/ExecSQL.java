package org.sandersc.util.db.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.sandersc.util.db.CSVReportResultSetExtractor;
import org.sandersc.util.db.ConnectionManager;
import org.sandersc.util.db.JdbcTemplate;
import org.sandersc.util.text.Munger;

public class ExecSQL {
	
	public static final String DEFAULT_STATEMENT_DELIM = ";";
	public static final String DEFAULT_DATA_DIR_PATH = ".";
	public static final boolean DEFAULT_FORCE_OVERWRITE = true;
	public static final boolean DEFAULT_QUIET = false;
	public static final boolean DEFAULT_PRINT_HEADER = true;
	public static final boolean DEFAULT_MUNGE = false;
	
	private String statementDelim = DEFAULT_STATEMENT_DELIM;
	private String fieldSeparator = CSVReportResultSetExtractor.DEFAULT_FIELD_SEP;
	private String dataDirPath = DEFAULT_DATA_DIR_PATH;
	private Map<String,String> lobNameColumnMap = new HashMap<String,String>();
	private boolean forceOverwrite = DEFAULT_FORCE_OVERWRITE;
	private boolean quiet = DEFAULT_QUIET;
	private boolean printHeader = DEFAULT_PRINT_HEADER;
	private boolean munge = DEFAULT_MUNGE;
	private Properties mungeProperties = new Properties();
	private List<String> sqlStatements = new ArrayList<String>();
	private PrintStream output = System.out;
	private PrintStream error = System.err;
	
	public String getStatementDelimeter() {
		return statementDelim;
	}
	
	public void setStatementDelimeter(String statementDelim) {
		this.statementDelim = statementDelim;
	}
	
	public String getFieldSeparator() {
		return fieldSeparator;
	}
	
	public void setFieldSeparator(String fieldSeparator) {
		this.fieldSeparator = fieldSeparator;
	}	
	
	public String getDataDirPath() {
		return dataDirPath;
	}
	
	public void setDataDirPath(String dataDirPath) {
		this.dataDirPath = dataDirPath;
	}
	
	public PrintStream getOutputStream() {
		return output;
	}

	public void setOutputStream(PrintStream out) {
		this.output  = out;
	}
	
	public PrintStream getErrorStream() {
		return error;
	}

	public void setErrorStream(PrintStream err) {
		this.error  = err;
	}	

	public boolean isForceOverwrite() {
		return forceOverwrite;
	}

	public void setForceOverwrite(boolean forceOverwrite) {
		this.forceOverwrite = forceOverwrite;
	}
	
	public boolean isQuiet() { 
		return quiet;
	}
	
	public void setQuiet(boolean quiet) {
		this.quiet = quiet;
	}
	
	public boolean isPrintHeader() {
		return this.printHeader;
	}

	public void setPrintHeader(boolean printHeader) {
		this.printHeader = printHeader;
	}

	public boolean isMunge() {
		return munge;
	}

	public void setMunge(boolean munge) {
		this.munge = munge;
	}	
	
	public List<String> addSqlFilePath(String sqlFilePath) throws Exception {
		File sqlFile = new File(sqlFilePath);
		String sql = null;
		
		if( isMunge() ) {
			sql = Munger.munge(sqlFile, getMungeProperties());
		} else {
			sql = FileUtils.readFileToString(sqlFile, Charset.defaultCharset());
		}
		
		String [] statements = sql.split( getStatementDelimeter() );
		for( String statement : statements ) { 
			if( ! statement.trim().isEmpty() ) {
				sqlStatements.add(statement);
			}
		}
			
		return sqlStatements;
	}
	
	public List<String> getSqlStatements() {
		return sqlStatements;
	}
	
	public Properties getMungeProperties() {
		return mungeProperties;
	}

	public void setMungeProperties(Properties mungeProperties) {
		this.mungeProperties = mungeProperties;
	}
	
	public Map<String,String> getLobNameColumnMap() {
		return lobNameColumnMap;
	}

	public void setLobNameColumnMap(Map<String,String> lobNameColumnMap) {
		this.lobNameColumnMap = lobNameColumnMap;
	}	

	private void usage() {
		throw new IllegalArgumentException("Usage: execSQL [--quiet] [--noheader] [--munge] [--properties a=b,c=d,...] [--propFile <filename>] [--delimeter <sqldelim>] [--fieldSep <fieldSep>] [--datadir <dir>] [--lobColumnNames lobcolumn=filenamecolumn,...] [--overwrite] [--dbConfig <configFilePath>] <sqlfile> ...");
	}
	
	private String getNextArgument(String [] args, int index) {
		if( index >= args.length ) {
			System.err.println(String.format("Missing parameter value %s", args[args.length]));
			usage();
		} 
		
		return args[index];
	}
	
	public void parseArgs(String [] args) throws Exception {
		for( int i=0; i<args.length; i++ ) {
			if( args[i].equals("--datadir") ) { 
				setDataDirPath( getNextArgument(args, ++i) );
			} else if( args[i].equals("--lobColumnNames") ) {
				setLobNameColumnMap( parseLobNameColumnMap( getNextArgument(args, ++i) ));
			} else if( args[i].equals("--delimeter") ) { 
				setStatementDelimeter( getNextArgument(args, ++i) );
			} else if( args[i].equals("--fieldSep") ) {
				setFieldSeparator( getNextArgument(args, ++i) );
			} else if( args[i].equals("--overwrite") ) {
				setForceOverwrite( true );
			} else if( args[i].equals("--quiet") ) {
				setQuiet( true );
			} else if( args[i].equals("--noheader") ) {
				setPrintHeader( false );
			} else if( args[i].equals("--munge") ) {
				setMunge( true );
			} else if( args[i].equals("--properties") ) {
				String propString = getNextArgument(args, ++i);
				getMungeProperties().putAll(parseProperties(propString));
				setMunge( true );
			} else if( args[i].equals("--propFile") ) {
				String propFileName = getNextArgument(args, ++i);
				getMungeProperties().putAll(loadProperties(propFileName));
				setMunge( true );
			} else if( args[i].equals("--dbConfig") ) {
				String configFilePath = getNextArgument(args, ++i);
				System.setProperty(ConnectionManager.DB_CONFIG_PATH, configFilePath);
			} else if( args[i].startsWith("-") ){
				System.err.println(String.format("Invalid argument %s", args[i]));
				usage();
			} else {
				addSqlFilePath( args[i] );
			}
		}
		
		if( getSqlStatements().size() == 0 ) {
			usage();
		}
	}
	
	private Map<String,String> parseLobNameColumnMap(String encoded) {
		
		Map<String,String> result = new HashMap<String,String>();
		
		String [] parts = encoded.split(",");
		for( String part : parts ) {
			String [] nameValuePair = part.trim().split("=");
			if( nameValuePair.length != 2 ) { 
				System.err.println("Invalid value for clobNameMap parameter");
				usage();
			} else {
				result.put(nameValuePair[0].trim(),  nameValuePair[1].trim());
			}
		}
		
		return result;
	}

	private Properties loadProperties(String propFileName) throws IOException {
		
		Properties p = new Properties();
		InputStream is = new FileInputStream(propFileName);
		try { 
			p.load(is);
		} finally {
			is.close();
		}
		
		return p;
	}

	private Properties parseProperties(String propString) {
		
		Properties p = new Properties();
		
		String [] components = propString.split(",");
		for( String nameValuePair : components ) {
			String[] pair = nameValuePair.trim().split("=");
			if( pair.length == 2 ) {
				p.put(pair[0].trim(), pair[1].trim());
			} else {
				throw new IllegalArgumentException("Invalid name/value pair - more than one equals sign.");
			}
		}
		
		return p;
	}

	public void execute(String sql) throws Exception {
		String statement = sql;
		
		if( isMunge() ) {
			StringReader sr = new StringReader(sql);
			statement = Munger.munge(sr, getMungeProperties());
		}
		
		getSqlStatements().add(statement);
		execute();
	}
	
	public void execute() throws Exception {
		for( String sql : sqlStatements ) { 
	
			if( ! isQuiet() ) { 
				error.println(sql);
			}
	
			Connection conn = ConnectionManager.getConnection();
			try {
	
				JdbcTemplate template = new JdbcTemplate();
				template.setConnection(conn);
				if( sql.trim().matches("(?is)(select|with).*")) {
					CSVReportResultSetExtractor extractor = new CSVReportResultSetExtractor(getOutputStream());
					extractor.setDataDirPath(getDataDirPath());
					extractor.setLobNameColumnMap(getLobNameColumnMap());
					extractor.setOverwrite(isForceOverwrite());
					extractor.setPrintHeader(isPrintHeader());
					extractor.setFieldSeparator(getFieldSeparator());
					
					template.query(sql, extractor);
				} else { 
					int rowsAffected = template.update(sql);
					getOutputStream().println("Rows affected: " + rowsAffected);
				}
	
			} finally {
				conn.close();
			}
		}
	}	
	
	public static void main(String[] args) throws Exception {
		ExecSQL execSQL = new ExecSQL();
		execSQL.parseArgs(args);
		execSQL.setOutputStream(System.out);
		execSQL.setErrorStream(System.err);
		execSQL.execute();
	}
}
