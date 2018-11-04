import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class Generator {
	private static int MIN = 0;
	private static int HT_MAX   = 99999;
	private static int TT_MAX   = 9999;
	private static int OT_MAX   = 999;
	private static int HUND_MAX = 99;
	private static int TEN_MAX  = 9;
	// 4 bytes per integer, 5 integers, need 236 more bytes to make the row 256 bytes
	private static int STR_LEN = 236;

	public static List<TableRow> generateRows(int numRows) {
	    List<TableRow> rows = new ArrayList<TableRow>();
	    Random r = new Random();
	    for (int i = 0; i < numRows; i++){
	        TableRow row = new TableRow();
	        row.pk = UUID.randomUUID().toString();
	        row.ht = r.nextInt( (HT_MAX - MIN) + 1) + MIN;
            row.tt = r.nextInt( (TT_MAX - MIN) + 1) + MIN;
            row.ot = r.nextInt( (OT_MAX - MIN) + 1) + MIN;
            row.hund = r.nextInt( (HUND_MAX - MIN) + 1) + MIN;
            row.ten = r.nextInt( (TEN_MAX - MIN) + 1) + MIN;
            row.filler = RandomStringUtils.randomAlphanumeric(STR_LEN);
            rows.add(row);
        }
        return rows;
	}
}
