package DavisBase;
import java.io.RandomAccessFile;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

public class selectCommand {
	private String command = null;
	
	utility util = new utility();

	public selectCommand(String command) {
		this.command = command;		
	}
	
	public boolean execute() {
		if(command != null) {
			return parseSelectString();
		} else {
			System.out.println("ERROR: command is empty");
			return false;
		}
	}
	public boolean parseSelectString() {
		ArrayList<String> selectCommandSplit = new ArrayList<String>(Arrays.asList(command.split(" ")));
		if (selectCommandSplit.size() > 4) {
			if ((selectCommandSplit.get(0).trim().equals("select") && selectCommandSplit.get(1).trim().equals("*") && selectCommandSplit.get(2).trim().equals("from"))
				&& selectCommandSplit.get(4).trim().equals("where") && selectCommandSplit.get(5).trim().equals("row_id") && selectCommandSplit.get(6).trim().equals("=")) {
				return parseSelectWhereCommand();
			}
			System.out.println("Invalid select command");
			return false;
		}
		else if ((selectCommandSplit.get(0).trim().equals("select") && selectCommandSplit.get(1).trim().equals("*") && selectCommandSplit.get(2).trim().equals("from"))) {			
			return parseSelectCommand();
		}
		else {
			System.out.println("Invalid select command");
			return false;
		}
	
	}
	public boolean parseSelectWhereCommand() {
		try {
			ArrayList<String> selectCommandSplit = new ArrayList<String>(Arrays.asList(command.split(" ")));
			String tableName = selectCommandSplit.get(3).trim();
			int rowId = Integer.parseInt(selectCommandSplit.get(7).trim());
			if (util.isTablePresent(tableName, true)) {
				RandomAccessFile table = new RandomAccessFile("data/user_data/" + tableName + ".tbl", "rw");
				if (table.length() > 0L) {
					table.seek(1);
					int children = table.readShort();
					int count = 0;
					int lastRecordRowId = 0;
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
						table.seek(512);
					}
					else {
					table.seek(table.getFilePointer() - 18);	
					lastRecordRowId = table.readByte();
					table.readLong();
					table.readByte();
					long startPage = table.readLong();
					table.seek(startPage);
					}					
					java.util.List<column> cols = util.getTableCols(tableName);
					table.readByte();
					
					int cells = table.readByte();
					
					table.readShort();
					long rightPointer = table.readInt();
					ArrayList<Short> cellPointers = new ArrayList<Short>();
					Short pointer;
					for (int i = 0; i < cells; i++) {
						pointer = table.readShort();
						//if (pointer != 0){
						cellPointers.add(Short.valueOf(pointer));
						//}
					}
					boolean nextPage = true;
					for (column col : cols) {
						System.out.print(col.getColumnName() + "\t\t");
					}
					System.out.println();
					int colNum = cols.size();
					int row;
					int isValid = 1;
					ArrayList<Byte> valLength = new ArrayList<Byte>();
					Byte lengthPointer;
					//while (nextPage) {
						for (int i = lastRecordRowId + 1; i <= rowId; i++) {
							isValid = cellPointers.get(i - lastRecordRowId - 1);
							table.seek(((Short) cellPointers.get(i - lastRecordRowId - 1)).shortValue());	
						
						}
						if (isValid == 0) {
							System.out.println("Invalid Row ID");
							return false;
						}
						table.readShort();//payload
						row = table.readInt();
						table.readByte();
						System.out.print(row + "\t\t");
						for(int j = 0; j < colNum - 1; j++) {
							lengthPointer = table.readByte();
							valLength.add(Byte.valueOf(lengthPointer));							
						}
						for(int j = 0; j < colNum - 1; j++) {
							if (valLength.get(j) == 0x06) {
								System.out.print(table.readInt() + "\t\t");
							} else if (valLength.get(j) == (0x04)) {
								System.out.print(table.readByte() + "\t\t");
							} else if (valLength.get(j) == (0x05)) {
								System.out.print(table.readShort() + "\t\t");
							} else if (valLength.get(j) == (0x07)) {
								System.out.print(table.readLong() + "\t\t");
							} else if (valLength.get(j) == (0x08)) {
								System.out.print(table.readFloat() + "\t\t");
							} else if (valLength.get(j) == (0x09)) {
								System.out.print(table.readDouble() + "\t\t");
							} else if (valLength.get(j) == (0x0B)) {
								System.out.print(util.convertDateToString(table.readLong()) + "\t\t");
							} else if (valLength.get(j) == (0x0A)) {
								System.out.print(util.convertDateTimeToString(table.readLong()) + "\t\t");
							} else if(valLength.get(j)  > 0x0C) {
								//int length = table.readByte();
								int length = valLength.get(j) - 0x0C;
								byte[] bytes = new byte[length];
								table.read(bytes, 0, bytes.length);
								System.out.print(new String(bytes) + "\t\t");
							}								
						}
						System.out.println();	/*					
						if (rightPointer != -1) {
							table.seek(rightPointer);
							table.readByte();
							cells = table.readByte();
							table.readShort();
							rightPointer = table.readInt();
							cellPointers = new ArrayList<Short>();
							for (int i = 0; i < cells; i++) {
								cellPointers.add(Short.valueOf(table.readShort()));
							}
						} else {
							nextPage = false;
						}*/
					
				} else {
					System.out.println("Table is empty");
				}
				table.close();
				return true;
			}
		} catch (Exception e) {
			System.out.println("ERROR");
		}
		return false;
	}
	
	public boolean parseSelectCommand() {
		try {
			ArrayList<String> selectCommandSplit = new ArrayList<String>(Arrays.asList(command.split(" ")));
			String tableName = selectCommandSplit.get(3).trim();
			if (util.isTablePresent(tableName, true)) {
				RandomAccessFile table = new RandomAccessFile("data/user_data/" + tableName + ".tbl", "rw");
				if (table.length() > 0L) {
					java.util.List<column> cols = util.getTableCols(tableName);
					for (column col : cols) {
						System.out.print(col.getColumnName() + "\t\t");
					}
					System.out.println();
					table.seek(0);
					table.readByte();
					int children = table.readShort();					
					long recordsAddress;
					long currLocation;
					for(int k = 0; k < children; k++) {
						
						table.readByte();
						recordsAddress = table.readLong();
						currLocation = table.getFilePointer();
						table.seek(recordsAddress);
						table.readByte();
						int cells = table.readByte();
						table.readShort();
						long rightPointer = table.readInt();
						ArrayList<Short> cellPointers = new ArrayList<Short>();
						Short pointer;
						for (int i = 0; i < cells; i++) {
							pointer = table.readShort();
							if (pointer != 0){
							cellPointers.add(Short.valueOf(pointer));
							}
						}
						int colNum = cols.size();
						int rowId;
						int value;
						ArrayList<Byte> valLength = new ArrayList<Byte>();
						Byte lengthPointer;
						for (int i = 0; i < cellPointers.size(); i++) {
							table.seek(((Short) cellPointers.get(i)).shortValue());
							table.readShort();//payload
							rowId = table.readInt();
							table.readByte();
							System.out.print(rowId + "\t\t");
							for(int j = 0; j < colNum - 1; j++) {
								lengthPointer = table.readByte();
								valLength.add(Byte.valueOf(lengthPointer));							
							}
							int val;
							for(int j = 0; j < colNum - 1; j++) {
								if (valLength.get(j) == 0x06) {									
									val = table.readInt();
									if (val == 2) {
										System.out.print("null" + "\t\t");
									} else {
										System.out.print(val + "\t\t");
									}									
								} else if (valLength.get(j) == (0x04)) {
									val = table.readByte();
									if (val == 0) {
										System.out.print("null" + "\t\t");
									} else {
										System.out.print(val + "\t\t");
									}
								} else if (valLength.get(j) == (0x05)) {
									val = table.readShort();
									if (val == 1) {
										System.out.print("null" + "\t\t");
									} else {
										System.out.print(val + "\t\t");
									}									
								} else if (valLength.get(j) == (0x07)) {
									System.out.print(table.readLong() + "\t\t");
								} else if (valLength.get(j) == (0x08)) {
									float valF = table.readFloat();
									if (valF == 2.0) {
										System.out.print("null" + "\t\t");
									} else {
										System.out.print(valF + "\t\t");
									}
								} else if (valLength.get(j) == (0x09)) {
									double valD = table.readDouble();
									if (valD == 3.0) {
										System.out.print("null" + "\t\t");
									} else {
										System.out.print(valD + "\t\t");
									}
								} else if (valLength.get(j) == (0x0B)) {
									long valL = table.readLong();
									if (valL == 3) {
										System.out.print("null" + "\t\t");
									} else {
										Date date = new Date(valL);
										SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
										String dateFormatted = formatter.format(date);
										System.out.print(dateFormatted + "\t\t");
									}																	
								} else if (valLength.get(j) == (0x0A)) {															        
									long valL = (int) table.readLong();
									if (valL == 3) {
										System.out.print("null" + "\t\t");
									} else {
										Date date = new Date(valL);
										SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
										String dateFormatted = formatter.format(date);
										System.out.print(dateFormatted + "\t\t");
									}									
								} else if(valLength.get(j)  > 0x0C) {									
									int length = valLength.get(j) - 0x0C;
									byte[] bytes = new byte[length];
									table.read(bytes, 0, bytes.length);
									System.out.print(new String(bytes) + "\t\t");
								}					
							}
							System.out.println();
							valLength.clear();
						}
						table.seek(currLocation);
					}
				} else {
					System.out.println("No record present");
				}
				table.close();
				return true;
			}
		} catch (Exception e) {
			System.out.println("ERROR");
		}
		return false;
	}
}
