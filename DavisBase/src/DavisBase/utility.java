package DavisBase;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;



public class utility {
	
	
	public utility() {
	}
		
public List<column> getTableCols(String tableName) {
		
	List<column> columns = new java.util.ArrayList<column>();
		try {
			if (isTablePresent(tableName, true)) {
			
				RandomAccessFile table1 = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
				table1.seek(0);
				table1.read();
				long len = table1.length();
				long poi = table1.getFilePointer();
				//while (table1.getFilePointer() < table1.length()) {
				while (poi < len) {
					table1.readByte();
					
					byte length = table1.readByte();
					byte[] bytes = new byte[length];
					table1.read(bytes, 0, bytes.length);
					String[] column = new String(bytes).replaceAll("#", " ").split(" ");
					if ((column[2].equals(tableName))) {

						column col = new column();
						col.setColumnName(column[3]);
						col.setDataType(column[4]);
						col.setPrimary(false);
						col.setNotNullable(false);
						
							if (column[5].equals("PRI")) {
								col.setPrimary(true);
							} else if (column[6].equals("NO")) {
								col.setNotNullable(true);
							}
						
						columns.add(col);
					}
					table1.readInt();
					poi = table1.getFilePointer();
				}
				table1.close();
			}
		} catch (Exception e) {
			System.out.println("Error");
		}

		return columns;
	}

	public boolean isTablePresent(String tableName, boolean showMessage) {
		try {
			File file = new File("data/user_data/" + tableName + ".tbl");
			if ((file.exists()) && (!file.isDirectory()))
				return true;
			if (showMessage) {
				System.out.println("Table " + tableName + " is not present");
			}
		} catch (Exception e) {
			return false;
		}
	
		return false;
	}
	
	public String convertDateToString(long date) {
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date d = new Date(date);
		return format.format(d);
	}

	public String convertDateTimeToString(long date) {
		String pattern = "YYYY-MM-DD_hh:mm:ss";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date d = new Date(date);
		return format.format(d);
	}


	
}
