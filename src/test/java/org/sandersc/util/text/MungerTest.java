package org.sandersc.util.text;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.Properties;

import org.junit.Test;

public class MungerTest {

	@Test
	public void testMultipleLines() throws Exception {
		
		Properties p = new Properties();
		p.setProperty("DBSCHEMA", "CSANDERS");
		p.setProperty("NEGATION", "not enforced");
		
		String input = "alter table &DBSCHEMA;.tabname \n alter foreign key y &NEGATION;;";
		String output = "";
		
		StringReader sr = new StringReader(input);
		try {
			output = Munger.munge(sr, p);
		} finally {
			sr.close();
		}
		
		assertEquals("alter table CSANDERS.tabname \n alter foreign key y not enforced;\n", output);
	}

}
