package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

public class Load {
	DownloadFileNAS downloadFile;
	String sql = "";
	PreparedStatement pst = null;
	ResultSet rs = null;
	static final String NUMBER_REGEX = "^[0-9]+$";
	private Connection connectionDB1;
	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
	LocalDateTime now = LocalDateTime.now();
	String timestamp = dtf.format(now);
	// //==jdbc:mysql://localhost:3306/dwh_1
	private static String USER = "root";
	private static String PASS = "";

	private static String hostName = "localhost";
	private static String driver = "jdbc:mysql:";
	private static String addressConfig = "localhost:3306/control_db";
	private static String addressSource = "localhost:3306/datawarehouse";
	private static String dbName = "myconfig";
	private static Connection con;
	private static Connection conn;

	private String localPath = "D:\\LAB_DW";
//	String tableName;
//	String fileType;
//	int numOfcol;
//	int numOfdata; // có 20 sv ==> = 20
//	ArrayList<String> columnsList = new ArrayList<String>();
//	int numberColumns;
//	static String dataPath;
//	String delimiter;

	public Connection createConnection1() {
		try {
//			Class.forName("com.mysql.cj.jdbc.Driver");
			Class.forName("com.mysql.jdbc.Driver");
			try {
				conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/control_db", USER, PASS);
			} catch (SQLException e) {
				System.out.println("Tai khoan hoac mat khau sai");
				e.printStackTrace();
				System.exit(0);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
//			System.exit(0);
		}
		return conn;

	}

	public void insertDataLog() {
		int rs = 0;
		sql = "INSERT INTO log (file_name_,data_file_config_id,file_status,staging_load_count, timestamp_download, timestamp_insert_staging, timestamp_insert_datawarehouse)"
				+ " values (?,?,?,?,?,?,?)";
		File local = new File(localPath);
		File[] listFileLog = local.listFiles();
		for (int i = 1; i < listFileLog.length; i++) {
			try {
				pst = createConnection1().prepareStatement(sql);
				pst.setString(1, listFileLog[i].getName());
				pst.setInt(2, 1);
				pst.setString(3, "ER");
				pst.setInt(4, 0);
				pst.setString(5, timestamp);
				pst.setString(6, null);
				pst.setString(7, null);
				rs = pst.executeUpdate();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if (pst != null)
						pst.close();
					if (this.rs != null)
						this.rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

			}
		}
		System.out.println("Down on Log Successfully!");

	}

	// private static String connectionURL = "jdbc:mysql://" + hostName +
//	public void createConnection() throws SQLException {
//		System.out.println("Connecting database....");
//		try {
//			Class.forName("com.mysql.jdbc.Driver");
//			con = DriverManager.getConnection(driver + "//" + addressConfig, USER, PASS);
//			conn = DriverManager.getConnection(driver + "//" + addressSource, USER, PASS);
//			System.out.println("Complete!!!!!");
//			System.out.println("----------------------------------------------------------------");
//		} catch (ClassNotFoundException e) {
//			System.out.println("Can't connect!!!!!!!!!!");
//			System.out.println("----------------------------------------------------------------");
//		}
//	}

//	public void getConfig(int id) throws SQLException {
//		PreparedStatement pre = (PreparedStatement) con.prepareStatement("SELECT * FROM controldb.config where id=?;");
//		pre.setInt(1, id);
//		ResultSet tmp = pre.executeQuery();
//		tmp.next();
//		tableName = tmp.getString("tableName");
//		numOfcol = Integer.parseInt(tmp.getString("numOfCol"));
//		String listofcol = tmp.getString("listField");
//		dataPath = tmp.getString("dataPath");
//		// URL db đưa lên:: jdbc:mysql://localhost:3306/control_db
//		// dấu phân cách
//		delimiter = tmp.getString("delimiter");
//		StringTokenizer tokens = new StringTokenizer(listofcol, delimiter);
//		while (tokens.hasMoreTokens()) {
//			columnsList.add(tokens.nextToken());
//		}
//		System.out.println("Get config: complete!!!!");
//	}

	// truyền dấu phân cách , đường dẫn file ở đâu
	public void load(String delimiter, String pathFile) throws ClassNotFoundException, SQLException, IOException {
		System.out.println("Connect DB Successfully :)");
		File f = new File(pathFile);
		if (!f.exists()) {
			System.out.println("File not exist!");
			return;
		}
		BufferedReader lineReader = new BufferedReader(new FileReader(f));
		String lineText = null;
		int count = 0;
		String sql;
		lineText = lineReader.readLine();
		String[] fields = lineText.split(delimiter);
		System.out.println(fields.length);
		System.out.println(lineText);
	}

	// đọc dữ liệu
//	public void readData() throws IOException, EncryptedDocumentException, InvalidFormatException {
//		System.out.println("Connect DB Successfully Read file:)");
//		File f = new File(dataPath);
//		System.out.println(f);
//		if (!f.exists()) {
//			System.out.println("File not exist!");
//			return;
//		}
//		// File file = new File(f);//lấy đường dẫn file từ db config xuống
//		BufferedReader lineReader = new BufferedReader(new FileReader(f));
//		String lineText = null;
//		int count = 0;
//		String sql;
//		lineText = lineReader.readLine();
//		System.out.println(lineText);
//		FileInputStream fis = new FileInputStream(f);
//	}

	// |
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

	//
//	public String sqlCreatTable() {
//		String preSql = "CREATE TABLE " + tableName + " (";
//		preSql += columnsList.get(0) + " INT PRIMARY KEY NOT NULL,";
//		for (int i = 1; i < numOfcol; i++) {
//			preSql += columnsList.get(i) + " VARCHAR(100),";
//		}
//		preSql = preSql.substring(0, preSql.length() - 1) + ");";
//		System.out.println(preSql);
//		// idSV,name,DateOfBirth,gender,phone
//		System.out.println("creat thành công");
//		return preSql;
//	}
//
//	public String insertData() throws SQLException {
//		String preSql = "INSERT INTO " + tableName + "(";
//		for (int i = 0; i < numOfcol; i++) {
//			preSql += columnsList.get(i) + ",";
//		}
//		preSql = preSql.substring(0, preSql.length() - 1) + ") VALUES   " + readValuesTXT(dataPath, delimiter) + ";";
//
//		System.out.println("............Rd ...");
//		System.out.println(preSql);
//		System.out.println("INSERT  thành công");
//		return preSql;
//
//	}

//	public void extractData() throws SQLException {
//		String sqlCreateTb = sqlCreatTable();
//
//		String sqlInsertData = insertData();
//		System.out.println("String sqlInsertData = insertData ();");
//		System.out.println(sqlInsertData);
//		// create table
//		boolean tableStatus = false;
//		boolean readDataStatus = false;
//
//		try {
//			System.out.println("Creating table " + tableName + ".......");
//			PreparedStatement state = conn.prepareStatement(sqlCreateTb);
//			System.out.println(state);
//			state.execute();
//			tableStatus = true;
//			System.out.println("Create Table: Complete!!!");
//			System.out.println("----------------------------------------------------------------");
//		} catch (Exception e) {
//			System.out.println("Can't create table " + tableName);
//			System.out.println("----------------------------------------------------------------");
//		}
//		if (tableStatus) {
//			try {
//				System.out.println("Insert  table " + tableName + ".......");
//				PreparedStatement stateInsert = conn.prepareStatement(sqlInsertData);
//				System.out.println(stateInsert);
//				stateInsert.execute();
//				tableStatus = true;
//				System.out.println("Insert Table: Complete!!!");
//				System.out.println("----------------------------------------------------------------");
//			} catch (Exception e) {
//				System.out.println("Can't Insert table " + tableName);
//				System.out.println("----------------------------------------------------------------");
//			}
//
//		}
//
//	}

	public static void main(String[] args)
			throws SQLException, IOException, EncryptedDocumentException, InvalidFormatException {
		Load ex = new Load();
		Load load = new Load();
		System.out.println(load.createConnection1());
		load.insertDataLog();
//		ex.createConnection();
//		ex.getConfig(1);
//		System.out.println("READ data");
//		System.out.println(ex.sqlCreatTable());// đọc file csv đưa vào cả các trường luôn
//		System.out.println("test VTT");
//		System.out.println("exit test VTT");
//		System.out.println("tới phần gây cấn nhất insert ");
//		ex.insertData();
//		ex.extractData();
//		System.out.println("OK ");

	}
}
