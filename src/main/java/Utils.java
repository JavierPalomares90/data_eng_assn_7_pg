import java.sql.*;
import java.util.List;

public class Utils {
	//@formatter:off
	private static String INSERT_COLS = "(pk,ht,tt,ot,hund,ten,filler) VALUES(?,?,?,?,?,?,?)";
	private static String CREATE_TABLE_COLS = "(" +
            "    pk     STRING PRIMARY KEY," +
            "    ht     INTEGER," +
            "    tt     INTEGER," +
            "    ot     INTEGER," +
            "    hund   INTEGER," +
            "    ten    INTEGER," +
            "    filler STRING (236)" +
            ");";
	private static String DROP_TABLE_PREFIX = "DROP TABLE ";
	private static String COLUMN_A_INDEX = "CREATE INDEX columnA_index on benchmark(columnA)";
	private static String COLUMN_B_INDEX = "CREATE INDEX columnB_index on benchmark(columnB)";
	private static String COLUMN_A_B_INDEX = "CREATE INDEX columnA_B_index on benchmark(columnA,columnB)";

	private static String DROP_COLUMN_A_INDEX = "DROP INDEX columnA_index";
	private static String DROP_COLUMN_B_INDEX = "DROP INDEX columnB_index";
	private static String DROP_COLUMN_A_B_INDEX = "DROP INDEX columnA_B_index";

	private static String QUERY_1 = "SELECT * FROM benchmark WHERE benchmark.columnA = ?";
	private static String QUERY_2 = "SELECT * FROM benchmark WHERE benchmark.columnB = ?";
	private static String QUERY_3 = "SELECT * FROM benchmark WHERE benchmark.columnA = ? AND benchmark.columnB = ?";

	//@formatter:on

	public static int NUM_ROWS   = 5000000;
	public static int BATCH_SIZE = 50000;

	public static int[] columnAVals = {10301,23,308,7785,45898,867,73,88,343,234};
	public static int[] columnBVals = {18775,3564,87,4787,5,92,345,48998,12,9};

	private static String url = "jdbc:sqlite:C:/Users/javie/Documents/Data_Engineering/hw2.db";

	public static void createDB(String filename){
		url = "jdbc:sqlite:" + filename;
		try {
			Connection conn = DriverManager.getConnection(url);
			if (conn != null) {
				DatabaseMetaData meta = conn.getMetaData();
				System.out.println("The driver name is " + meta.getDriverName());
				System.out.println("A new database has been created.");
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	public static void load(){
		try
		{
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			System.err.println("Unable to find driver");
		}
	}

	public static double createTable(String tableName){
		Connection connection = Utils.connect(url);
		long startTime = System.nanoTime();
		Utils.createTable(connection,tableName);
		long endTime = System.nanoTime();
		long executionTime = endTime - startTime;
		Utils.closeConnection(connection);
		return executionTime;
	}

	public static double insertData(String tableName, List<TableRow> rows){
		Connection connection = Utils.connect(url);
		long startTime = System.nanoTime();
		Utils.insertBatch(connection,tableName,rows);
		long endTime = System.nanoTime();
		long executionTime = endTime - startTime;
		double seconds = (double) executionTime/ 1000000000.0;
		Utils.closeConnection(connection);
		return executionTime;
	}

	public static double createIndexA(){
		Connection connection = Utils.connect(url);
		long startTime = System.nanoTime();
		Utils.createIndexColumnA(connection);
		long endTime = System.nanoTime();
		long executionTime = endTime - startTime;
		double seconds = (double) executionTime/ 1000000000.0;
		Utils.closeConnection(connection);
		return  executionTime;
	}

	public static double createIndexB()
	{
		Connection connection = Utils.connect(url);
		long startTime = System.nanoTime();
		Utils.createIndexColumnB(connection);
		long endTime = System.nanoTime();
		long executionTime = endTime - startTime;
		double seconds = (double) executionTime / 1000000000.0;
		Utils.closeConnection(connection);
		return executionTime;
	}

	public static double createIndexAB(){
		Connection connection = Utils.connect(url);
		long startTime = System.nanoTime();
		Utils.createIndexColumnAB(connection);
		Utils.closeConnection(connection);
		long endTime = System.nanoTime();
		long executionTime = endTime - startTime;
		double seconds = (double) executionTime / 1000000000.0;
		Utils.closeConnection(connection);
		return executionTime;
	}

	public static double query1(){
		Connection connection = Utils.connect(url);
		int numLoops = Utils.columnAVals.length;
		double executionTime = 0.0;
		for(int i = 0; i < numLoops; i++){
			int n = Utils.columnAVals[i];
			long startTime = System.nanoTime();
			Utils.query1(connection,n);

			long endTime = System.nanoTime();
			long t = endTime - startTime;
			executionTime += (double) t;

		}
		executionTime = executionTime / (1.0 * numLoops);
		Utils.closeConnection(connection);
		return executionTime;
	}

	public static double query2(){
		Connection connection = Utils.connect(url);
		int numLoops = Utils.columnAVals.length;
		double executionTime = 0.0;
		for(int i = 0; i < numLoops; i++){
			int n = Utils.columnBVals[i];
			long startTime = System.nanoTime();
			Utils.query2(connection,n);

			long endTime = System.nanoTime();
			long t = endTime - startTime;
			executionTime += (double) t;
		}
		executionTime = executionTime / (1.0 * numLoops);
		Utils.closeConnection(connection);
		return executionTime;
	}

	public static double query3(){
		Connection connection = Utils.connect(url);
		int numLoops = Utils.columnBVals.length;
		double executionTime = 0.0;
		for(int i = 0; i < numLoops; i++){
			int n1 = Utils.columnAVals[i];
			int n2 = Utils.columnBVals[i];
			long startTime = System.nanoTime();
			Utils.query3(connection,n1,n2);

			long endTime = System.nanoTime();
			long t = endTime - startTime;
			executionTime += (double) t;

		}
		executionTime = executionTime / (1.0 * numLoops);
		Utils.closeConnection(connection);
		return executionTime;
	}

	public static void dropTable(String tableName)
	{
		Connection connection = Utils.connect(url);
		Utils.dropTable(connection,tableName);
		Utils.closeConnection(connection);
	}

	public static void dropIndexA(){
		Connection connection = Utils.connect(url);
		Utils.dropIndexA(connection);
		Utils.closeConnection(connection);

	}

	public static void dropIndexB(){
		Connection connection = Utils.connect(url);
		Utils.dropIndexB(connection);
		Utils.closeConnection(connection);

	}

	public static void dropIndexAB(){
		Connection connection = Utils.connect(url);
		Utils.dropIndexAB(connection);
		Utils.closeConnection(connection);

	}
	private static Connection connect(String url) {
		Connection conn = null;
		try {
			// db parameters
			// Change this to the appropriate path
			// create a connection to the database
			conn = DriverManager.getConnection(url);

			System.out.println("Connection to SQLite has been established.");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return conn;
	}

	private static void closeConnection(Connection connection){
		if(connection != null){
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	private static void createTable(Connection conn,String tableName){
		try
		{
			Statement stmt = conn.createStatement();
			stmt.execute("CREATE TABLE " + tableName + CREATE_TABLE_COLS);

		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

	private static void dropTable(Connection conn,String tableName){
		try
		{
            conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(DROP_TABLE_PREFIX + tableName);
			conn.commit();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}

	}

	private  static void executeSql(Connection conn, String sql){
		PreparedStatement statement = null;
		try
		{
			statement = conn.prepareStatement(sql);
			statement.executeUpdate();
		} catch (SQLException e)
		{
			e.printStackTrace();
		} finally {

			if(statement!=null){
				try
				{
					statement.close();
				}catch (SQLException e){
					System.err.println(e.getMessage());
				}
			}
		}

	}

	private static void dropIndexAB(Connection conn){
		if(conn == null){
			return;
		}
		executeSql(conn,DROP_COLUMN_A_B_INDEX);
	}

	private static void dropIndexA(Connection conn){
		if(conn == null){
			return;
		}
		executeSql(conn,DROP_COLUMN_A_INDEX);
	}

	private static void dropIndexB(Connection conn){
		if(conn == null){
			return;
		}
		executeSql(conn,DROP_COLUMN_B_INDEX);
	}


	private static void createIndexColumnAB(Connection conn){
		if(conn == null){
			return;
		}
		executeSql(conn,COLUMN_A_B_INDEX);
	}

	private static void createIndexColumnA(Connection connn){
	    if(connn == null){
	    	return;
		}
		executeSql(connn,COLUMN_A_INDEX);
	}

	private static void createIndexColumnB(Connection conn){
		if(conn == null){
			return;
		}
		executeSql(conn,COLUMN_B_INDEX);

	}

	private static void query3(Connection connection,int value1, int value2){
		if(connection == null){
			return;
		}
		PreparedStatement statement = null;
		ResultSet rs = null;
		try{
			statement = connection.prepareStatement(QUERY_3);
			statement.setInt(1,value1);
			statement.setInt(2, value2);
			rs = statement.executeQuery();
		}catch (SQLException e){
		    System.err.print(e.getMessage());
		}finally
		{
			if(statement!=null){
				try
				{
					statement.close();
				}catch (SQLException e){
					System.err.println(e.getMessage());
				}
			}
		}
	}

	private static void query2(Connection connection, int value){
		if(connection == null){
			return;
		}
		PreparedStatement statement = null;
		ResultSet rs = null;
		try{
			statement = connection.prepareStatement(QUERY_2);
			statement.setInt(1,value);
			rs = statement.executeQuery();

		}catch (SQLException e){
			System.err.print(e.getMessage());
		}finally
		{
			if(statement!=null){
				try
				{
					statement.close();
				}catch (SQLException e){
					System.err.println(e.getMessage());
				}
			}
		}
	}

	private static void query1(Connection connection,int value){
		if(connection == null){
			return;
		}
		PreparedStatement statement = null;
		ResultSet rs = null;
		try{
			statement = connection.prepareStatement(QUERY_1);
			statement.setInt(1,value);
			rs = statement.executeQuery();
		}catch (SQLException e){
			System.err.print(e.getMessage());
		}finally
		{
			if(statement!=null){
				try
				{
					statement.close();
				}catch (SQLException e){
					System.err.println(e.getMessage());
				}
			}
		}
	}

	private static void insertBatch(Connection conn,String tableName, List<TableRow> rows){
		if(conn == null){
			return;
		}
		// set auto-commit mode to false
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}
		int count = 0;
		PreparedStatement statement = null;
		try {
            statement = conn.prepareStatement("INSERT INTO " + tableName + INSERT_COLS);
            for(TableRow row: rows)
			{
			    statement.setString(1,row.pk);
				statement.setInt(2, row.ht);
				statement.setInt(3, row.tt);
				statement.setInt(4, row.ot);
				statement.setInt(5, row.hund);
				statement.setInt(6, row.ten);
				statement.setString(7, row.filler);
				statement.addBatch();
				count++;
				if (count % BATCH_SIZE == 0)
				{
					System.out.println("Commit the batch of size " + BATCH_SIZE);
					int[] result = statement.executeBatch();
					System.out.println("Number of rows inserted: " + result.length);
					conn.commit();
				}
			}

        }catch (SQLException e){
            System.err.println(e.getMessage());
            return;
        }finally
		{

		    if(statement!=null){
		        try
				{
					statement.close();
				}catch (SQLException e){
					System.err.println(e.getMessage());
				}
			}
		}

	}
}
