package connection;

import java.sql.Connection;
import java.sql.DriverManager;

public class ConnectionBD {
	public static Connection getConnection(String db_name) {
		String host = "jdbc:mysql://localhost:3307/ " +  db_name;
		String username = "root";
		String password = "1234";

		Connection myConnection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");

			myConnection = DriverManager.getConnection(host, username, password);

		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
		return myConnection;

	}

}
