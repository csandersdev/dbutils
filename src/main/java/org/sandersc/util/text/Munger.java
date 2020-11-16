package org.sandersc.util.text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

/**
 * Simple text manipulation utility that will process input line-by-line
 * substituting any entity references with the corresponding values from a given
 * properties file.
 * 
 * @author sandersc
 */
public class Munger {
	
	private static final String BEGIN_ENTITY = "&";
	private static final String END_ENTITY = ";";

	public static String munge(String filePath, Properties p) throws Exception {
		File file = new File(filePath);
		return munge(file, p);
	}

	public static String munge(File file, Properties p) throws Exception {
		InputStream is = new FileInputStream(file);
		try {
			return munge(is, p);
		} finally {
			is.close();
		}
	}

	public static String munge(InputStream is, Properties p) throws Exception {
		Reader r = new InputStreamReader(is);
		return munge(r, p);
	}
	
	public static String munge(Reader r, Properties p) throws Exception {
		StringBuffer mungedData = new StringBuffer("");
		
		BufferedReader br = new BufferedReader(r);
		String line;
		while ((line = br.readLine()) != null) {
			for (Object obj : p.keySet()) {
				String property = (String) obj;
				line = line.replaceAll(BEGIN_ENTITY + property + END_ENTITY,
						p.getProperty(property));
			}
			mungedData.append(line);
			mungedData.append("\n");
		}

		return mungedData.toString();		
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: munge <propertiesFile> <inputFile>");
			System.exit(1);
		}

		String propFilePath = args[0];
		String inputFilePath = args[1];

		Properties p = new Properties();

		File propFile = new File(propFilePath);
		InputStream is = new FileInputStream(propFile);
		try {
			p.load(is);
		} finally {
			is.close();
		}

		File inputFile = new File(inputFilePath);
		is = new FileInputStream(inputFile);
		try {
			System.out.println(Munger.munge(is, p));
		} finally {
			is.close();
		}
	}
}
