package org.sandersc.util.db.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.sandersc.util.db.CSVReportResultSetExtractor;

public class ExecSQLTest {
	
	private String lineSep = System.getProperty("line.separator");

	
	@Test
	public void testSuccessNoResultsFound() throws Exception {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
		PrintStream ps = new PrintStream(baos);
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.setOutputStream(ps);
		execSQL.execute("select schemaid, tablename from sys.systables where 1=0");

		
		String lineSep = System.getProperty("line.separator");
		
		StringBuffer sb = new StringBuffer();
		sb.append("SCHEMAID");
		sb.append(CSVReportResultSetExtractor.DEFAULT_FIELD_SEP);
		sb.append("TABLENAME");
		sb.append(lineSep);
		sb.append("No results found.");
		sb.append(lineSep);
		
		String expected = sb.toString();
		String actual = baos.toString();
		assertEquals(expected, actual);
	}
	
	@Test
	public void testSuccessResultsFound() throws Exception {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
		PrintStream ps = new PrintStream(baos);
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.setOutputStream(ps);
		execSQL.execute("select tablename from SYS.SYSTABLES where tablename in ( 'SYSTABLES', 'SYSKEYS' ) order by tablename");
		
		String lineSep = System.getProperty("line.separator");
		
		StringBuffer sb = new StringBuffer();
		sb.append("TABLENAME");
		sb.append(lineSep);
		sb.append("SYSKEYS");
		sb.append(lineSep);
		sb.append("SYSTABLES");
		sb.append(lineSep);
		
		String expected = sb.toString();
		String actual = baos.toString();
		assertEquals(expected, actual);
	}
	
	@Test
	public void testSuccessFileWithCRLF() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
		PrintStream ps = new PrintStream(baos);
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.setOutputStream(ps);
		execSQL.addSqlFilePath("target/test-classes/test_with_crlf.sql");
		execSQL.execute();
		
		String lineSep = System.getProperty("line.separator");
		
		StringBuffer sb = new StringBuffer();
		sb.append("GREETING");
		sb.append(CSVReportResultSetExtractor.DEFAULT_FIELD_SEP);
		sb.append("PARTING");
		sb.append(lineSep);
		sb.append("hello, world");
		sb.append(CSVReportResultSetExtractor.DEFAULT_FIELD_SEP);
		sb.append("goodbye");
		sb.append(lineSep);
		
		String expected = sb.toString();
		String actual = baos.toString();
		assertEquals(expected, actual);		
	}
	
	@Test
	public void testSuccessFileWithNoHeaderOutput() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
		PrintStream ps = new PrintStream(baos);
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.setPrintHeader(false);
		execSQL.setOutputStream(ps);
		execSQL.addSqlFilePath("target/test-classes/test_with_crlf.sql");
		execSQL.execute();
		
		String lineSep = System.getProperty("line.separator");
		
		StringBuffer sb = new StringBuffer();
		sb.append("hello, world");
		sb.append(CSVReportResultSetExtractor.DEFAULT_FIELD_SEP);
		sb.append("goodbye");
		sb.append(lineSep);
		
		String expected = sb.toString();
		String actual = baos.toString();
		assertEquals(expected, actual);		
	}
	
	@Test
	public void testSuccessFileUsingCustomDelimeter() throws Exception {
		ByteArrayOutputStream baosOut = new ByteArrayOutputStream(); 
		PrintStream out = new PrintStream(baosOut);
		
		ByteArrayOutputStream baosErr = new ByteArrayOutputStream();
		PrintStream err = new PrintStream(baosErr);
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.setPrintHeader(false);
		execSQL.setStatementDelimeter("%");
		execSQL.setOutputStream(out);
		execSQL.setErrorStream(err);		
		execSQL.addSqlFilePath("target/test-classes/test_with_percent_delimeter.sql");
		execSQL.execute();
		
		String lineSep = System.getProperty("line.separator");
		
		StringBuffer sb = new StringBuffer();
		sb.append("insert into TESTTAB ( PROP ) values ( VALUE );");
		sb.append(lineSep);
		
		String expected = sb.toString();
		String actual = baosOut.toString();
		assertEquals(expected, actual);		
		
		StringBuffer sbErr = new StringBuffer();
		sbErr.append("select 'insert into TESTTAB ( PROP ) values ( VALUE );' \"SQL\" from SYS.SYSTABLES where tablename = 'SYSTABLES' ");
		sbErr.append(lineSep);
		
		String expectedErr = sbErr.toString();
		String actualErr = baosErr.toString();
		assertEquals(expectedErr, actualErr);
	}
	
	@Test
	public void testSuccessFileQuietNoSQL() throws Exception {
		ByteArrayOutputStream baosOut = new ByteArrayOutputStream(); 
		PrintStream out = new PrintStream(baosOut);
		
		ByteArrayOutputStream baosErr = new ByteArrayOutputStream();
		PrintStream err = new PrintStream(baosErr);
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.setPrintHeader(false);
		execSQL.setStatementDelimeter("%");
		execSQL.setQuiet(true);
		execSQL.setOutputStream(out);
		execSQL.setErrorStream(err);
		execSQL.addSqlFilePath("target/test-classes/test_with_percent_delimeter.sql");
		execSQL.execute();
		
		String lineSep = System.getProperty("line.separator");
		
		StringBuffer sb = new StringBuffer();
		sb.append("insert into TESTTAB ( PROP ) values ( VALUE );");
		sb.append(lineSep);
		
		String expected = sb.toString();
		String actual = baosOut.toString();
		assertEquals(expected, actual);		
		
		assertEquals(0, baosErr.size());
	}
	
	@Test
	public void testParseArgsSuccess() throws Exception {
		String [] args = new String[] { "--quiet", "--noheader", "target/test-classes/test_with_percent_delimeter.sql" };
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.parseArgs(args);
		
		assertEquals(false, execSQL.isPrintHeader());
		assertEquals(true, execSQL.isQuiet());
	}
	
	@Test
	@Ignore // not working in Derby and need to figure out why
	public void testLobOutputSuccess() throws Exception {
		
		File outputDir = new File("target/test-classes");
		
		File expectedOutputFile = new File(outputDir, "hello_world.txt");
		expectedOutputFile.delete();
		
		String [] args = new String [] { "--datadir", outputDir.getAbsolutePath(), "--lobColumnNames", "LOB_TEXT=LOB_FILENAME,C=D", "target/test-classes/test_lob_output.sql" };
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.parseArgs(args);
		
		assertEquals(outputDir.getAbsolutePath(), execSQL.getDataDirPath());
		assertEquals(2, execSQL.getLobNameColumnMap().size());
		assertEquals("LOB_FILENAME", execSQL.getLobNameColumnMap().get("LOB_TEXT"));
		assertEquals("D", execSQL.getLobNameColumnMap().get("C"));
		
		execSQL.execute();
		
		assertTrue("LOB file was not generated or not generated with the expected name", expectedOutputFile.exists());
		
		String contents = FileUtils.readFileToString(expectedOutputFile, Charset.defaultCharset());
		assertEquals("Hello, World!", contents);
	}
	
	@Test
	public void testMungePropertiesFileSQLStringProperties() throws Exception {
		ByteArrayOutputStream baosOut = new ByteArrayOutputStream(); 
		PrintStream out = new PrintStream(baosOut);
		
		ByteArrayOutputStream baosErr = new ByteArrayOutputStream();
		PrintStream err = new PrintStream(baosErr);		
		
		String [] args = new String[] { "--munge", "--properties", "SCHEMA=myschema,NEGATION=not enforced", "target/test-classes/test_with_munge.sql" };
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.setOutputStream(out);
		execSQL.setErrorStream(err);
		execSQL.parseArgs(args);
		
		assertTrue(execSQL.isMunge());
		assertEquals(2, execSQL.getMungeProperties().size());
		
		execSQL.execute();
		
		StringBuffer sb = new StringBuffer();
		sb.append("SCHEMA");
		sb.append(lineSep);
		sb.append("myschema");
		sb.append(lineSep);
		
		String expected = sb.toString();
		String actual = baosOut.toString();
		assertEquals(expected, actual);
	}
	
	@Test
	public void testMungePropertiesInlineSQLFileProperties() throws Exception {
		ByteArrayOutputStream baosOut = new ByteArrayOutputStream(); 
		PrintStream out = new PrintStream(baosOut);
		
		ByteArrayOutputStream baosErr = new ByteArrayOutputStream();
		PrintStream err = new PrintStream(baosErr);		
		
		String [] args = new String[] { "--munge", "--propFile", "target/test-classes/testMunger.properties", "target/test-classes/test_with_munge.sql" };
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.setOutputStream(out);
		execSQL.setErrorStream(err);
		execSQL.parseArgs(args);
		
		execSQL.getSqlStatements().clear();
		
		assertTrue(execSQL.isMunge());
		assertEquals(1, execSQL.getMungeProperties().size());
		
		execSQL.execute("select '&SCHEMA;' \"SCHEMA\" from SYS.SYSTABLES where TABLENAME = 'SYSTABLES'");
		
		StringBuffer sb = new StringBuffer();
		sb.append("SCHEMA");
		sb.append(lineSep);
		sb.append("SYS");
		sb.append(lineSep);
		
		String expected = sb.toString();
		String actual = baosOut.toString();
		assertEquals(expected, actual);
	}
	
	@Test
	public void testOverrideFieldSeparator() throws Exception {
		String fieldSep = "\t";
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
		PrintStream out = new PrintStream(baos);
		
		
		String [] args = new String[] { "--fieldSep", fieldSep, "target/test-classes/test_with_crlf.sql" };
		
		ExecSQL execSQL = new ExecSQL();
		execSQL.setOutputStream(out);
		execSQL.parseArgs(args);
		
		execSQL.execute();
		
		String lineSep = System.getProperty("line.separator");
		
		StringBuffer sb = new StringBuffer();
		sb.append("GREETING");
		sb.append(fieldSep);
		sb.append("PARTING");
		sb.append(lineSep);
		sb.append("hello, world");
		sb.append(fieldSep);
		sb.append("goodbye");
		sb.append(lineSep);
		
		String expected = sb.toString();
		String actual = baos.toString();
		assertEquals(expected, actual);	
	}	
}
