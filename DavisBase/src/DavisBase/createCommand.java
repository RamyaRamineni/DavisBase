package DavisBase;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;



public class createCommand {

	private String command = null;	
	static long pageSize = 512L;
	public createCommand(String command) {
		this.command = command;		
	}

	
	public boolean execute() {
		if(command == null) {
			System.out.println("ERROR: Command is empty");
			return false;
		}
		return parseCreateString();
	}
	
	private boolean parseCreateString() {
 		Pattern createTablePattern = Pattern.compile("^create\\stable\\s([^(]*)\\((.*)\\)$");
		Matcher commandMatcher = createTablePattern.matcher(command);
		if(commandMatcher.find()) {
			try {
				ArrayList<String> createCommandSplit = new ArrayList<String>(Arrays.asList(command.split(" ")));
				
				String tableName = createCommandSplit.get(2);
				String columnStringPart = commandMatcher.group(2).trim();
				String[] columnsStrings = columnStringPart.split(",");
				File file = new File("data/user_data/" + tableName + ".tbl");
				
				if (!file.exists()) {
					
					RandomAccessFile tables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
					
					tables.seek(0);					
					int key;
					key = tables.read();//read number of records count																																																
					if (key == -1) {
					
						key = 0;
					}
					tables.seek(0);
					tables.write(key+1);// write count + 1 at position 0
					tables.seek(tables.length());
					tables.write(key+1);//number specific to the table which is being inserted	//row_id
					tables.writeByte(1);//validity flag
					tables.writeByte(tableName.length());//write length of the new table name
					tables.writeBytes(tableName);//write table name
					//tables.writeInt(columnNumber);
					tables.writeInt(0);//separator
					tables.close();
					
					RandomAccessFile columns = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
					int i = 1;//i is record count with respect to table
					for (String subString : columnsStrings) {
						columns.seek(0);
						key = columns.read();																																																
						if (key == -1) {
							key = 0;
						}	
						columns.seek(0);
						columns.write(key+1);//record count
						columns.seek(columns.length());
						int num = key +1;//record number //row_id
						subString = subString.trim();
						if ((subString != null) && (!subString.isEmpty())) {
							columns.writeByte(1);//validity flag
							if (subString.contains("primary key")) {
								subString = subString.replace("primary key", "PRI");
								subString = subString + " NO";
							}
							else if(!(subString.contains("primary key")) && subString.contains("not null")) {
								subString = subString + "NULL";								
								subString = subString.replace("not null", "");
								subString = subString + " NO";													
							}
							else {
								subString = subString + " NULL";
								subString = subString + " YES";
							}
							
							
							String columnDefination = num+"#"+i+"#"+tableName + "#"
									+ subString.replaceAll(" ", "#").trim();
							columns.writeByte(columnDefination.length());
							columns.writeBytes(columnDefination);
							columns.writeInt(0);//separator
							i = i+1;
						}
					}
					columns.close();
					
					RandomAccessFile table = new RandomAccessFile("data/user_data/" + tableName + ".tbl", "rw");
					table.close();
					System.out.println("Table is created Successfully");
				} else {
					System.out.println("Table already exists");
				}
			} catch (Exception e) {
				System.out.println("ERROR");
			}

						return true;
		}
		else {
			System.out.println("I didn't understand the command: \"" + command + "\"");
			return false;
		}
	}


	
}
	