package utils;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class AddData {
	FileConfiguration configuration = new FileConfiguration();
	PreparedStatement preparedStatement = null;

	public void addData() throws Exception {
		File file = new File(configuration.selectField("pathFile", "config"));
		Class.forName("com.mysql.cj.jdbc.Driver");
		Connection connection = DriverManager.getConnection(configuration.selectField("source", "config"),
				configuration.selectField("userName", "config"), configuration.selectField("password", "config"));
		///

		FileInputStream inputStream = new FileInputStream(file);
		


		Workbook workbook = new XSSFWorkbook(inputStream);
		Sheet sheet = workbook.getSheetAt(0);
		Iterator<Row> rowIterator = sheet.iterator();
		rowIterator.next();

		String sql_createtable = "CREATE TABLE " + configuration.selectField("tableName", "config")
				+ " (STT INT NOT NULL auto_increment, MSSV INT NOT NULL, holot VARCHAR(255) NOT NULL, ten VARCHAR(255) NOT NULL, ngaysinh date NOT NULL, malop VARCHAR(8), tenLop VARCHAR(255), dtlienlac INT NOT NULL, email VARCHAR(255) NOT NULL, quequan VARCHAR(255) NOT NULL, ghichu TEXT, PRIMARY KEY (STT));";

		preparedStatement = connection.prepareStatement(sql_createtable);
		preparedStatement.execute();

		String sql = "INSERT INTO " + configuration.selectField("tableName", "config")
				+ " (MSSV, holot, ten, ngaysinh, malop, tenLop, dtlienlac, email, queQuan, ghiChu) VALUES (?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement ps = connection.prepareStatement(sql);

		while (rowIterator.hasNext()) {
			Row nextRow = rowIterator.next();
			Iterator<Cell> cellIterator = nextRow.cellIterator();

			while (cellIterator.hasNext()) {
				Cell nextCell = cellIterator.next();

				int columnIndex = nextCell.getColumnIndex();

				switch (columnIndex) {
				case 1:
					int mssv = (int) nextCell.getNumericCellValue();
					ps.setInt(1, mssv);
					break;
				case 2:
					String holot = nextCell.getStringCellValue();
					ps.setString(2, holot);
					break;
				case 3:
					String ten = nextCell.getStringCellValue();
					ps.setString(3, ten);
					break;
				case 4:
					Date ngaysinh = (Date) nextCell.getDateCellValue();
					ps.setString(4, getString(ngaysinh));
					break;
				case 5:
					String malop = nextCell.getStringCellValue();
					ps.setString(5, malop);
					break;
				case 6:
					String tenLop = nextCell.getStringCellValue();
					ps.setString(6, tenLop);
					break;
				case 7:
					int dtlienlac = (int) nextCell.getNumericCellValue();
					ps.setInt(7, dtlienlac);
					break;
				case 8:
					String email = nextCell.getStringCellValue();
					ps.setString(8, email);
					break;
				case 9:
					String queQuan = nextCell.getStringCellValue();
					ps.setString(9, queQuan);
					break;
				case 10:
					String ghiChu = nextCell.getStringCellValue();
					ps.setString(10, ghiChu);
					break;
				}

			}
			ps.execute();
		}
		workbook.close();
		inputStream.close();
		ps.close();
		connection.close();
	}

	public static String getString(Date d) {
		return new SimpleDateFormat("yyyy-MM-dd").format(d);
	}

}
