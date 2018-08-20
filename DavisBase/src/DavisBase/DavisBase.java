package DavisBase;

import java.io.RandomAccessFile;
import java.util.Scanner;



import java.util.ArrayList;
import java.util.Arrays;

import static java.lang.System.out;

import static java.lang.Math.toIntExact;



/**
 *  @author Ramya Ramineni
 *  @version 1.0
 *  <b>
 *  <p>This is an example of how to create an interactive prompt</p>
 *  <p>There is also some guidance to get started with read/write of
 *     binary data files using RandomAccessFile class</p>
 *  </b>
 *
 */
public class DavisBase {

	/* This can be changed to whatever you like */
	static String prompt = "davisql> ";
	static String version = "v1.0b(example)";
	static String copyright = "©2018 Ramya Ramineni";
	
	static boolean isExit = false;
	/*
	 * Page size for alll files is 512 bytes by default.
	 * You may choose to make it user modifiable
	 */
	static long pageSize = 512; 

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	/** ***********************************************************************
	 *  Main method
	 */
    public static void main(String[] args) {

		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = ""; 

		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");


	}

	/** ***********************************************************************
	 *  Static method definitions
	 */

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBase"); // Display the string.
		System.out.println("DavisBase Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80)); 
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}
	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}
	
		/**
		 *  Help: Display supported commands
		 */
		public static void help() {
			out.println(line("*",80));
			out.println("SUPPORTED COMMANDS\n");
			out.println("All commands below are case insensitive\n");
			out.println("SHOW TABLES;");
			out.println("\tDisplay the names of all tables.\n");
			//printCmd("SELECT * FROM <table_name>;");
			//printDef("Display all records in the table <table_name>.");
			out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
			out.println("\tDisplay table records whose optional <condition>");
			out.println("\tis <column_name> = <value>.\n");
			out.println("DROP TABLE <table_name>;");
			out.println("\tRemove table data (i.e. all records) and its schema.\n");
			out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
			out.println("\tModify records data whose optional <condition> is\n");
			out.println("VERSION;");
			out.println("\tDisplay the program version.\n");
			out.println("HELP;");
			out.println("\tDisplay this help information.\n");
			out.println("EXIT;");
			out.println("\tExit the program.\n");
			out.println(line("*",80));
		}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand) {
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		

		/*
		*  This switch handles a very small list of hardcoded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		switch (commandTokens.get(0)) {
			case "insert":
				System.out.println("CASE: INSERT");
				parseInsert(userCommand);
				break;
			case "show":		
				System.out.println("CASE: SHOW");				
				parseShowTables(userCommand);
				break;				
			case "select":
				System.out.println("CASE: SELECT");
				parseSelect(userCommand);
				break;
			case "drop":
				if (commandTokens.get(1).equals("table")) {
					System.out.println("CASE: DROP");
					dropTable(userCommand);
					break;
				}
				else {
					System.out.println("Invalid command");
					break;
				}
			case "create":
				System.out.println("CASE: CREATE");
				parseCreateTable(userCommand);
				break;
			case "update":
				System.out.println("CASE: UPDATE");
				parseUpdate(userCommand);
				break;
			case "delete":
				System.out.println("CASE: DELETE");
				parseDelete(userCommand);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}
	

	/**
	 *  Stub method for dropping tables
	 *  @param dropTableString is a String of the user input
	 */
	public static void dropTable(String dropTableString) {
		System.out.println("STUB: This is the dropTable method.");
		System.out.println("Parsing the string:\"" + dropTableString + "\"");
		try {
			RandomAccessFile tables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			//RandomAccessFile tables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			boolean isRecordPresent = false;
			ArrayList<String> dropTableSplit = new ArrayList<String>(Arrays.asList(dropTableString.split(" ")));
			String dropTableName = dropTableSplit.get(2).trim();
			tables.seek(2);
			while (tables.getFilePointer() < tables.length()) {
				
				int isNotDeleted = tables.read();
				int location = toIntExact(tables.getFilePointer());
				byte length = tables.readByte();
				byte[] bytes = new byte[length];
				tables.read(bytes, 0, bytes.length);
				String tableName = new String(bytes).trim();
				if (isNotDeleted == 1 && tableName.equals(dropTableName)) {
					//database.seek(database.getFilePointer() - bytes.length - 2);
					tables.seek(location-1);
					tables.write(0);
					System.out.println(dropTableName +" table is dropped successfully");
					isRecordPresent = true;
					break;
				}
				tables.readInt();
				tables.read();
				
			}
			if (!isRecordPresent)
				System.out.println("Table not available");
			tables.close();
		} catch (Exception e) {
			System.out.println("ERROR");
		}      
	}
	public static void parseInsert(String insertString) {
		System.out.println("STUB: This is the Insert method");
		System.out.println("Parsing the string:\"" + insertString + "\"");
		insertCommand insertIntoTableCommand = new insertCommand(insertString);
		insertIntoTableCommand.execute();
	}
	public static void parseDelete(String insertString) {
		System.out.println("STUB: This is the Delete method");
		System.out.println("Parsing the string:\"" + insertString + "\"");
		deleteCommand deleteFromTableCommand = new deleteCommand(insertString);
		deleteFromTableCommand.execute();
	}
	public static void parseSelect(String selectString) {
		System.out.println("STUB: This is the Select method");
		System.out.println("Parsing the string:\"" + selectString + "\"");
		selectCommand selectTableCommand = new selectCommand(selectString);
		selectTableCommand.execute();
	}
	
	/**
	 *  Stub method for executing queries
	 *  @param queryString is a String of the user input
	 */
	public static void parseQuery(String queryString) {
		System.out.println("STUB: This is the parseQuery method");
		System.out.println("\tParsing the string:\"" + queryString + "\"");
	}

	/**
	 *  Stub method for updating records
	 *  @param updateString is a String of the user input
	 */
	public static void parseUpdate(String updateString) {
		System.out.println("STUB: This is the dropTable method");
		System.out.println("Parsing the string:\"" + updateString + "\"");
		updateCommand updateTableCommand = new updateCommand(updateString);
		updateTableCommand.execute();
	}
	
	public static void parseShowTables(String showString) {
		System.out.println("STUB: This is the show tables method");
		System.out.println("Parsing the string:\"" + showString + "\"");
		try {
			RandomAccessFile tables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");			
			ArrayList<String> showCommandSplit = new ArrayList<String>(Arrays.asList(showString.split(" ")));
			if(showCommandSplit.get(1).equals("tables")){
				boolean isRecordPresent = false;
				tables.seek(2);
				while (tables.getFilePointer() < tables.length()) {					
					int isNotDeleted = tables.read();
					byte length = tables.readByte();
					byte[] bytes = new byte[length];
					tables.read(bytes, 0, bytes.length);
					if (isNotDeleted == 1) {
						isRecordPresent = true;
						System.out.println(new String(bytes));
					}
					tables.readInt();
					tables.read();					
				}
				if (!isRecordPresent) {
					System.out.println("No tables available");
					tables.close();
				}
			}
			else {
				System.out.println("I didn't understand the command: \"" + showString + "\"");
			}
		} catch (Exception e) {
			System.out.println("ERROR");
		}      

	}
	

	 
	/**
	 *  Stub method for creating new tables
	 *  @param queryString is a String of the user input
	 */
	public static void parseCreateTable(String createString) {
		
		System.out.println("STUB: Calling your method to create a table");
		System.out.println("Parsing the string:\"" + createString + "\"");
		//ArrayList<String> createCommandSplit = new ArrayList<String>(Arrays.asList(createString.split(" ")));

		/* Define table file name */
		//String tableFileName = createCommandSplit.get(2) + ".tbl";

		/* YOUR CODE GOES HERE */
		createCommand createTableCommand = new createCommand(createString);
		createTableCommand.execute();
	}
}