package DataBaseCreator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManager {
	
	private static final String URL = "jdbc:mysql://localhost:3306/Incidents";
	
	private static final String USERNAME = "root";
	private static final String PASSWORD = "root"; // Alternative: Root@123 or root
	private static Connection connection = null;
	
	public static Connection getConnection() {
		Connection conn = null;
		try {conn = DriverManager.getConnection(URL,USERNAME, PASSWORD);
		} catch(SQLException e) {
			e.printStackTrace();
			}
		return conn;
	}
	
	public static void makeConnection() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.cj.jdbc.Driver");
		connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
	};
	
}