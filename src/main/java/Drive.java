import java.util.List;

public class Drive
{
    private static String TAB = "\t\t";
    private static double NANOS_TO_SEC = 1000000000.0;

    public static void main(String[] args) {
	    if(args.length < 1){
		    System.out.println("Usage: Drive [db_save_location]");
		    return;
	    }
	    // get the save location of the filename
	    String filename = args[0];
	    Utils.load();
	    Utils.createDB(filename);

	    // Create the table
	    Utils.createTable("TestData");
	    // Insert the data with no index
	    List<TableRow> rows = Generator.generateRows(Utils.NUM_ROWS);
	    Utils.insertData("TestData",rows);

	    /*
	    Utils.createTable("A");
	    Utils.insertData("A",rows);

        Utils.createTable("B");
        Utils.insertData("B",rows);

        Utils.createTable("C");
        Utils.insertData("C",rows);

        Utils.createTable("A_prime");
        Utils.insertData("A_prime",rows);

        Utils.createTable("B_prime");
        Utils.insertData("B_prime",rows);

        Utils.createTable("C_prime");
        Utils.insertData("C_prime",rows);
        */
    }

}
