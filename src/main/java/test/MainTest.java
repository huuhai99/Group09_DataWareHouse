package test;

import utils.AddData;
import utils.FileConfiguration;
import utils.ReadValues;

public class MainTest {
	public static void main(String[] args) {
		try {

			FileConfiguration selectFieldInDB = new FileConfiguration();
			System.out.println(selectFieldInDB.selectField("tableName", "config"));

			AddData data = new AddData();
			data.addData();
			

			System.out.println("success");
		} catch (Exception e) {
			System.out.println("fail");
			e.printStackTrace();
		}
	}
}
