package DavisBase;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

public class deleteCommand {

	utility util = new utility();
	private String command = null;
	
	public boolean execute() {
		if(command != null) {
			return parseDeleteString();
		} else {
			System.out.println("ERROR: Command is empty");
			return false;
		}
	}
	
	public deleteCommand(String command) {
		this.command = command;		
	}

	public boolean parseDeleteString() {
		try {
			ArrayList<String> deleteCommandSplit = new ArrayList<String>(Arrays.asList(command.split(" ")));
			//String[] deleteCommandSplit = command.split(" ");
			if (!(deleteCommandSplit.get(0).trim().equals("delete") && deleteCommandSplit.get(1).trim().equals("from") && deleteCommandSplit.get(3).trim().equals("where") 
					&& deleteCommandSplit.get(4).trim().equals("row_id") && deleteCommandSplit.get(5).trim().equals("="))) {			
				System.out.println("Invalid delete command");
				return false;
			}
			String tableName = deleteCommandSplit.get(2).trim();
			if (util.isTablePresent(tableName, true)) {
				String filter = command.substring(command.indexOf("where") + 5, command.length()).trim();
				String[] filterArray = filter.split("=");
				RandomAccessFile table = new RandomAccessFile("data/user_data/" + tableName + ".tbl", "rw");
				
				

				if (table.length() > 0L) {
					int rowId = Integer.parseInt(filterArray[1].trim());
					table.seek(1);
					int children = table.readShort();
					int count = 0;
					int lastRecordRowId = 0;
					long startPage;
					for(int i = 0; i < children; i++) {
						count = count + 9;
					}
					table.seek(3 + count - 9);
					int highestRowId = table.readByte();
					if (rowId > highestRowId || rowId < 0) {
						System.out.println("Invalid Row ID");
						table.close();
						return false;
					}
					table.seek(3);
					int recordId = table.readByte();
					table.seek(3);
					while(!(rowId <= recordId )){
						recordId = table.readByte();
						table.readLong();
					}
					if (table.getFilePointer() == 3) {
						startPage = 512;
						table.seek(startPage);
					}
					else {
					table.seek(table.getFilePointer() - 18);	
					lastRecordRowId = table.readByte();
					table.readLong();
					table.readByte();
					startPage = table.readLong();
					table.seek(startPage);
					}
					
					
					
					
					
					
					
					
					
					
					//String rId = filterArray[1].trim();
					
					//table.seek(0);
					int pageType = table.readByte();
					//int recordCount = table.readByte();
					
					if (pageType == 13){
						table.seek(startPage + 8);//start of record location in header
						
						for (int i = 1; i < rowId; i++) {
							table.readShort();
						}
						int rowIdPointer = table.readShort();
						if (rowIdPointer != 0){
						table.seek(table.getFilePointer() - 2);
						table.writeShort(0);//removing pointer to the record
						table.close();
						System.out.println("Record is deleted successfully");
						return true;
						}
						else if (rowIdPointer == 0) {
							System.out.println("Record doesnt exist");
						}
						
					}
					//include non leaf node condition
					else {
						table.close();
						System.out.println("ERROR");
						return false;
					}
					
				} else {
					System.out.println("No records");
				}
				table.close();
			}
		} catch (Exception e) {
			System.out.println("ERROR");			
		}
		return false;
	}
}
