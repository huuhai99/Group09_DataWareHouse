package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import connection.ConnectionBD;

public class FileConfiguration {
	private ResultSet rs = null;
	PreparedStatement pst = null;
	Connection connection = null;
	
	public String selectField(String field, String tableName) throws Exception {
		String chosenField = null;
		connection = ConnectionBD.getConnection("controldb");
		String sql = "SELECT " + field + " FROM " + tableName;
		try {
			pst = connection.prepareStatement(sql);
			rs = pst.executeQuery();
			while(rs.next()) {
				chosenField = rs.getString(field);
			}
			return chosenField;
		} catch (Exception e) {
			return null;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if(rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}
}
