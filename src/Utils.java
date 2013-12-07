import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Utils {
	private static Logger LOGGER = Logger.getLogger("DAGR");
	
	private static Connection conn = null;
	
	static{
		Properties props = new Properties();
		InputStream iStream = Utils.class.getResourceAsStream("creds.properties");
		try {
			if(iStream==null){
				throw new IOException("resource creds.properties does not exist on the classpath");
			}
			props.load(iStream);
			iStream.close();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE,"Could not load properties", e);
			throw new ExceptionInInitializerError(e);
		}
		URL = props.getProperty("URL");
		user = props.getProperty("user");
		password = props.getProperty("password");
	}
	private static final String URL;
	private static final String user;
	private static final String password;
	
	public static boolean connect(){
		if(conn!=null){
			LOGGER.log(Level.WARNING,"connection already open");
			return false;
		}
		try{
			conn = DriverManager.getConnection("jdbc:mysql://" + URL + "?user=" + user + "&password="+password);
			return true;
		}
		catch(SQLException e){
			LOGGER.log(Level.SEVERE, "could not get connection", e);
			return false;
		}
	}
	
	public static void disconnnect(){
		if(conn==null){
			LOGGER.log(Level.WARNING,"close called on null connection");
			return;
		}
		try {
			conn.close();
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "could not close connection", e);
		}
	}
	
	public static void executeStatement(){
		
	}
	
	public static void executeQuery(){
		
	}
}
