package utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class ReadValues {
	
	
	static final String NUMBER_REGEX = "^[0-9]+$";
	int numOfcol;
	String tableName;
	ArrayList<String> columnsList = new ArrayList<String>();
	static String dataPath;
	String delimiter;

	private String readLines(String value, String delim) {
		String values = "";
		StringTokenizer stoken = new StringTokenizer(value, delim);
		if (stoken.countTokens() > 0) {
		}
		int countToken = stoken.countTokens();
		String lines = "(";
		for (int j = 0; j < countToken; j++) {
			String token = stoken.nextToken();
			if (Pattern.matches(NUMBER_REGEX, token)) {
				lines += (j == countToken - 1) ? token.trim() + ")," : token.trim() + ",";
			} else {
				lines += (j == countToken - 1) ? "'" + token.trim() + "')," : "'" + token.trim() + "',";
			}
			values += lines;
			lines = "";
		}
		return values;
	}

	public String readValuesTXT(String s_file, String delim) {
		String values = "";
		try {
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file)));
			String line;
			while ((line = bReader.readLine()) != null) {
				values += readLines(line, delim);
			}
			bReader.close();
			return values.substring(0, values.length() - 1);
		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String insertData() throws SQLException {
		String preSql = "INSERT INTO " + tableName + "(";
		for (int i = 0; i < numOfcol; i++) {
			preSql += columnsList.get(i) + ",";
		}
		preSql = preSql.substring(0, preSql.length() - 1) + ") VALUES   " + readValuesTXT(dataPath, delimiter) + ";";

		System.out.println("............Rd ...");
		System.out.println(preSql);
		System.out.println("INSERT  thành công");
		return preSql;

	}

}
