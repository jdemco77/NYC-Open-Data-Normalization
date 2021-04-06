package DataBaseCreator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class CreateDB {
	
	public static void createTables(){
		try (Connection conn = ConnectionManager.getConnection();
				 Statement stmt = conn.createStatement();
				){
				
				stmt.executeUpdate("drop database if exists Incidents;");
			    stmt.executeUpdate("CREATE DATABASE Incidents");
			    stmt.executeUpdate("use incidents;");
			    
			    stmt.execute("create table AddressTable (\r\n"
			    		+ "	   Address_id int primary key auto_increment,\r\n"
			    		+ "    Incident_zip varchar(255), \r\n"
			    		+ "    Incident_address varchar(255),\r\n"
			    		+ "    City varchar(255),\r\n"
			    		+ "    BBL varchar(255),\r\n"
			    		+ "    Cross_street_1 varchar(255),\r\n"
			    		+ "    Cross_street_2 varchar(255)\r\n"
			    		+ ");");
			    
			    stmt.execute("create table AddressesTable (\r\n"
			    		+ "	   Location int primary key auto_increment, \r\n"
			    		+ "    Address_id int,\r\n"
			    		+ "    foreign key(Address_id) references addressTable(Address_id)\r\n"
			    		+ "    );");
			    
			    stmt.execute("create table Community_Board_Borough(\r\n"
			    		+ "	   Community_Board varchar(255) primary key,\r\n"
			    		+ "    Borough varchar(255)\r\n"
			    		+ ");");
			    
			    stmt.execute("create table AgencyTable(\r\n"
			    		+ "	   Agency varchar(255) primary key ,\r\n"
			    		+ "    Agency_name varchar(255)\r\n"
			    		+ ");");
			    
			    stmt.execute("create table Complaint_category(\r\n"
			    		+ "	   Complaint int primary key auto_increment,\r\n"
			    		+ "    Descriptor varchar(255)\r\n"
			    		+ ");");
			    
			    stmt.execute("create table distress_Call(\r\n"
			    		+ "	Unique_key varchar(255) primary key ,\r\n"
			    		+ "    Location varchar(255),\r\n"
			    		+ "    x_coordinate varchar(255),\r\n"
			    		+ "    y_coordinate varchar(255),\r\n"
			    		+ "    Location_type varchar(255),\r\n"
			    		+ "    Community_Board varchar(255),\r\n"
			    		+ "    foreign key(Community_Board) references Community_Board_Borough(Community_board),\r\n"
			    		+ "    Complaint_type varchar(255),\r\n"
			    		+ "    Descriptor varchar(255),\r\n"
			    		+ "    Created_date varchar(255),\r\n"
			    		+ "    Closed_date varchar(255),\r\n"
			    		+ "    Call_status varchar(255),\r\n"
			    		+ "    Agency varchar(255),\r\n"
			    		+ "    foreign key(Agency) references AgencyTable(Agency),\r\n"
			    		+ "    Resolution_description text,\r\n"
			    		+ "    Resolution_action_updated_date varchar(255),\r\n"
			    		+ "    Open_data_channel_type varchar(255)\r\n"
			    		+ ");" );
			}catch(SQLException e) {
				e.printStackTrace();
				
			}
	}
	
	public static void insertIntoAddressTable(String Incident_zip,
												String Incident_address,
												String City,
												String BBL,
												String Cross_street_1,
												String Cross_street_2) {
	
		try (Connection conn = ConnectionManager.getConnection();
				 PreparedStatement pstmt = conn.prepareStatement("INSERT INTO AddressTable (Incident_zip,Incident_address,City,BBL,Cross_street_1,Cross_street_2) VALUES (?,?,?,?,?,?)");
				){
			pstmt.setString(1,Incident_zip );
			pstmt.setString(2,Incident_address);
			pstmt.setString(3,City);
			pstmt.setString(4,BBL);
			pstmt.setString(5,Cross_street_1);
			pstmt.setString(6,Cross_street_2);
			pstmt.executeUpdate();
			
		}catch(SQLException e) {
			e.printStackTrace();
		}
		}
	

	public static void insertIntoCommunityBoardBoroughTable(String Community_Board,String Borough) {
		try(Connection conn = ConnectionManager.getConnection();
				 PreparedStatement pstmt = conn.prepareStatement("INSERT into Community_Board_Borough (Community_Board,Borough) values(?,?)")){
					 
			pstmt.setString(1, Community_Board);
			pstmt.setString(2, Borough);
			pstmt.executeUpdate();
			
				 }catch(SQLIntegrityConstraintViolationException e) {
					 
				 }catch(SQLException e) {
					 e.printStackTrace();
				 }
	}
	
	public static void insertIntoAgencyTable(String Agency,String Agency_name) {
		try(Connection conn = ConnectionManager.getConnection();
				 PreparedStatement pstmt = conn.prepareStatement("INSERT into Agencytable(Agency,Agency_name) values(?,?)")){
			
			pstmt.setString(1, Agency);
			pstmt.setString(2, Agency_name);
			pstmt.executeUpdate();
			
				 }catch(SQLIntegrityConstraintViolationException e) {
					 
				 }catch(SQLException e) {
					 e.printStackTrace();
					 }
	}
	
	public static void insertIntoComplaintCategoryTable(String descriptor) {
		try(Connection conn = ConnectionManager.getConnection();
				 PreparedStatement pstmt = conn.prepareStatement("INSERT into Complaint_category(descriptor) values(?)")){
					 
			pstmt.setString(1, descriptor);
			pstmt.executeUpdate();
				 }catch(SQLIntegrityConstraintViolationException e) {
					 
				 }catch(SQLException e) {
					 e.printStackTrace();
				 }
	}
	
	public static void insertIntoDistressCallTable(String Unique_key,
													
													String x_coordinate,
													String y_coordinate,
													String location_type,
													String CommunityBoard,
													String complaint_type,
													String descriptor,
													String created_date,
													String closed_date,
													String call_status,
													String agency,
													String Resolution_description,
													String Resolution_action_updated_date,
													String open_data_channel_type) {

		try (Connection conn = ConnectionManager.getConnection();
		PreparedStatement pstmt = conn.prepareStatement("INSERT INTO distress_Call (Unique_key, x_coordinate,y_coordinate,location_type,Community_Board,complaint_type,descriptor,created_date,closed_date,call_status,agency,Resolution_description,Resolution_action_updated_date,open_data_channel_type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			){
			
			pstmt.setString(1, Unique_key);
			
			pstmt.setString(2, x_coordinate);
			pstmt.setString(3, y_coordinate);
			pstmt.setString(4, location_type);
			pstmt.setString(5, CommunityBoard);
			pstmt.setString(6, complaint_type);
			pstmt.setString(7, descriptor);
			pstmt.setString(8, created_date);
			pstmt.setString(9, closed_date);
			pstmt.setString(10, call_status);
			pstmt.setString(11, agency);
			pstmt.setString(12, Resolution_description);
			pstmt.setString(13, Resolution_action_updated_date);
			pstmt.setString(14, open_data_channel_type );
			pstmt.executeUpdate();
		}catch(SQLIntegrityConstraintViolationException e) {
			 
		}catch(SQLException e) {
			e.printStackTrace();
		}
}

	public static void main(String[] args) {
		try {
		File file = new File("C:\\Users\\james\\OneDrive\\Documents\\out.txt");
		FileReader in = new FileReader(file);
		BufferedReader reader = new BufferedReader(in);
		
		createTables();
		
		String line;
		while( (line = reader.readLine() ) != null ) {
			String[] ad = line.split("!*&");
			
				insertIntoAddressTable(ad[8],ad[9],ad[16],ad[24],ad[11],ad[12]);
				insertIntoCommunityBoardBoroughTable(ad[23],ad[25]);				
				insertIntoAgencyTable(ad[3],ad[4]);
				insertIntoComplaintCategoryTable(ad[6]);
				insertIntoDistressCallTable(ad[0],ad[26],ad[27],ad[7],ad[23],ad[5],ad[6],ad[1],ad[2],ad[19],ad[3],ad[21],ad[22],ad[28]);
			}
		
		
		reader.close();
		
			
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
			
		
}
}