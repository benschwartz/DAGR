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
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			throw new ExceptionInInitializerError(e);
		}
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
		connect();
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
			conn = null;
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "could not close connection", e);
		}
	}

	public static void executeStatement() {

	}

	public static void executeQuery() {

	}

	/**
	 * 
	 * @param GUID
	 * @param name
	 * @param create_date
	 * @param modify_date
	 * @param location
	 * @param parentGUID
	 * @param author
	 * @param type
	 * @param size
	 * @return GUID of the inserted result
	 */
	public static String insertDAGR(String GUID, String name, long create_date,
			long modify_date, String location, String parentGUID,
			String author, String type, long size) {
		GUID = encodeString(GUID);
		name = encodeString(name);
		location = encodeString(location);
		parentGUID = encodeString(parentGUID);
		author = encodeString(author);
		type = encodeString(type);
		String existingGUID = null;
		try {
			existingGUID = containsDAGR(name, location);
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE,
					"Could not determine if the object is in the DAGR", e);
			disconnnect();
			connect();
			return null;
		}
		if (existingGUID != null) {
			try {
				insertParentChild(parentGUID, existingGUID);
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE,
						"could not insert parent child relationship: {parent:"
								+ parentGUID + ", child:" + existingGUID + "}", e);
				disconnnect();
				connect();
			}
			LOGGER.warning(name + " is already present in the DAGR with GUID "
					+ existingGUID);

			return existingGUID;
		}
		try {
			Statement stmt = conn.createStatement();
			// AuthorID
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
				// LOGGER.info(insert);
				stmt.execute(insert);
				rs = stmt
						.executeQuery("Select author_id from author where name='"
								+ author + "';");
				if (!rs.next()) {
					rs.close();
					stmt.close();
					LOGGER.log(Level.SEVERE, "could not insert DAGR with GUID "
							+ GUID);
					return GUID;
				}
				authorID = rs.getInt("author_id");
				rs.close();

			}
			// Type
			rs = stmt.executeQuery("Select type from type where type = '"
					+ type + "'");
			if (!rs.next()) {
				stmt.execute("INSERT INTO type (type) VALUES ('" + type + "');");
			}
			rs.close();
			StringBuilder sql = new StringBuilder();
			sql.append("INSERT INTO dagr (GUID,name,date_created,date_modified,location,author_id,type,size) ");
			sql.append("VALUES (");
			sql.append("'" + GUID + "',");
			sql.append("'" + name + "',");
			sql.append(create_date + ",");
			sql.append(modify_date + ",");
			sql.append("'" + location + "',");
			sql.append(authorID + ",");
			sql.append("'" + type + "',");
			sql.append(size);
			sql.append(");");
			// LOGGER.info(sql.toString());
			stmt.execute(sql.toString());
			stmt.close();
			insertParentChild(parentGUID, GUID);
			LOGGER.info("INSERT " + location + " " + name + " " + GUID);
			return GUID;
		} catch (SQLException e) {
			String DAGR = location + " " + name + " " + GUID;
			LOGGER.log(Level.SEVERE, "Could not insert DAGR\n" + DAGR, e);
			disconnnect();
			connect();
			return null;
		}
	}

	private static void insertParentChild(String parentGUID, String GUID)
			throws SQLException {
		Statement stmt = conn.createStatement();
		StringBuilder sql = new StringBuilder();
		sql.append("Select count(*) COUNT from dagr_parent_child where ");
		sql.append("parent = '" + parentGUID + "'");
		sql.append(" AND child = '" + GUID + "';");
		ResultSet rs = stmt.executeQuery(sql.toString());
		rs.next();
		int count = rs.getInt("COUNT");
		if (count == 1) {
			LOGGER.warning("parent child relationship already exists {parent: "
					+ parentGUID + ", child: " + GUID + "}");
			rs.close();
			stmt.close();
			return;
		}
		rs.close();
		stmt.close();
		stmt = conn.createStatement();
		sql = new StringBuilder();
		sql.append("INSERT INTO dagr_parent_child (parent,child) ");
		sql.append("VALUES (");
		sql.append("'" + parentGUID + "',");
		sql.append("'" + GUID + "'");
		sql.append(");");
		stmt.execute(sql.toString());
		stmt.close();

	}

	private static String containsDAGR(String name, String location)
			throws SQLException {
		String existingGUID = null;
		Statement stmt = conn.createStatement();
		String sql = "SELECT guid FROM dagr where name='" + name
				+ "' AND location='" + location + "';";
		// LOGGER.info("Checking for: " + sql);
		ResultSet rs = stmt.executeQuery(sql);
		if (rs.next()) {
			existingGUID = rs.getString("guid");
		}
		rs.close();
		stmt.close();
		return existingGUID;
	}

	public static String getFileSystemUUID() {
		return "FS_ROOT";
	}

	private static String encodeString(String str) {
		return str.replace("\\", "\\\\").replace("'", "\'")
				.replace("\"", "\\\"");
	}
}
