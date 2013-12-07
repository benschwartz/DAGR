import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Utils {
	private static Logger LOGGER = Logger.getLogger("DAGR");

	private static Connection conn = null;

	static {
		Properties props = new Properties();
		InputStream iStream = Utils.class
				.getResourceAsStream("creds.properties");
		try {
			if (iStream == null) {
				throw new IOException(
						"resource creds.properties does not exist on the classpath");
			}
			props.load(iStream);
			iStream.close();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Could not load properties", e);
			throw new ExceptionInInitializerError(e);
		}
		URL = props.getProperty("URL");
		user = props.getProperty("user");
		password = props.getProperty("password");
	}
	private static final String URL;
	private static final String user;
	private static final String password;

	public static boolean connect() {
		if (conn != null) {
			LOGGER.log(Level.WARNING, "connection already open");
			return false;
		}
		try {
			conn = DriverManager.getConnection("jdbc:mysql://" + URL, user,
					password);
			return true;
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "could not get connection", e);
			return false;
		}
	}

	public static void disconnnect() {
		if (conn == null) {
			LOGGER.log(Level.WARNING, "close called on null connection");
			return;
		}
		try {
			conn.close();
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "could not close connection", e);
		}
	}

	public static void executeStatement() {

	}

	public static void executeQuery() {

	}

	public static void insertDAGR(String GUID, String name, String create_date, String modify_date,
			String location, String parentGUID, String author, String type,
			long size) {
		GUID = encodeString(GUID);
		name = encodeString(name);
		location = encodeString(location);
		parentGUID = encodeString(parentGUID);
		author = encodeString(author);
		type = encodeString(type);
		try {
			Statement stmt = conn.createStatement();
			//AuthorID
			int authorID = -1;
			ResultSet rs = stmt
					.executeQuery("Select author_id from author where name='"
							+ author + "';");
			if (rs.next()) {
				authorID = rs.getInt("author_id");
			} else {
				rs.close();
				String insert = "INSERT INTO author (name) VALUES ('" + author
						+ "');";
				LOGGER.info(insert);
				stmt.execute(insert);
				rs = stmt
						.executeQuery("Select author_id from author where name='"
								+ author + "';");
				if (!rs.next()) {
					rs.close();
					stmt.close();
					LOGGER.log(Level.SEVERE, "could not insert DAGR with GUID "
							+ GUID);
					return;
				}
				authorID = rs.getInt("author_id");
				rs.close();
			
			}
			// Type
			rs = stmt.executeQuery("Select type from type where type = '" + type + "'");
			if(!rs.next()){
				stmt.execute("INSERT INTO type (type) VALUES ('" + type + "');");
			}
			rs.close();
			StringBuilder sql = new StringBuilder();
			sql.append("INSERT INTO dagr (GUID,name,date_created,date_modified,location,parent_GUID,author_id,type,size) ");
			sql.append("VALUES (");
			sql.append("'" + GUID + "',");
			sql.append("'" + name + "',");
			sql.append(create_date + ",");
			sql.append(modify_date + ",");
			sql.append("'" + location + "',");
			sql.append("'" + parentGUID + "',");
			sql.append(authorID + ",");
			sql.append("'" + type + "',");
			sql.append(size);
			sql.append(");");
			LOGGER.info(sql.toString());
			stmt.execute(sql.toString());
			stmt.close();
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Could not insert DAGR with GUID " + GUID,
					e);
		}
	}

	public static String getFileSystemUUID() {
		return "FS_ROOT";
	}
	
	private static String encodeString(String str){
		return str.replace("\\", "\\\\").replace("'","\'").replace("\"", "\\\"");
	}
}
