package DavisBase;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class updateCommand {
	private String command = null;
	
	utility util = new utility();

	public updateCommand(String command) {
		this.command = command;		
	}
	
	public boolean execute() {
		if(command != null) {
			return parseUpdateString();
		} else {
			System.out.println("ERROR: command is empty");
			return false;
		}
	}
	public boolean parseUpdateString() {
		ArrayList<String> updateCommandSplit = new ArrayList<String>(Arrays.asList(command.split(" ")));
		if ((updateCommandSplit.get(0).trim().equals("update") && updateCommandSplit.get(2).trim().equals("set") && updateCommandSplit.get(4).trim().equals("=") && 
				updateCommandSplit.get(6).trim().equals("where") && updateCommandSplit.get(7).trim().equals("row_id")&& updateCommandSplit.get(8).trim().equals("="))) {			
			return parseUpdateCommand();
		}
		else {
			System.out.println("Invalid update command");
			return false;
		}
	
	}
	public boolean parseUpdateCommand() {
		try {

			ArrayList<String> updateCommandSplit = new ArrayList<String>(Arrays.asList(command.split(" ")));
			String tableName = updateCommandSplit.get(1).trim();
			int rowId = Integer.parseInt(updateCommandSplit.get(9).trim());
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
							table.close();
							return false;
						}
						table.readShort();//payload
						row = table.readInt();
						table.readByte();
						
						for(int j = 0; j < colNum - 1; j++) {
							lengthPointer = table.readByte();
							valLength.add(Byte.valueOf(lengthPointer));							
						}
					int colCount = 0;
					int i = 0;
					for (column col : cols) {	
						if (updateCommandSplit.get(3).trim().equals(col.getColumnName())) {
							colCount++;
							colNum = colCount - 1;
								if (valLength.get(i - 1) == 0x06) {
									table.writeInt(Integer.parseInt(updateCommandSplit.get(5).trim()));
								} else if (valLength.get(i - 1) == (0x04)) {
									table.writeByte(Byte.parseByte(updateCommandSplit.get(5).trim()));
								} else if (valLength.get(i - 1) == (0x05)) {
									table.writeShort(Short.parseShort(updateCommandSplit.get(5).trim()));
								} else if (valLength.get(i - 1) == (0x07)) {
									table.writeLong(Long.parseLong(updateCommandSplit.get(5).trim()));
								} else if (valLength.get(i - 1) == (0x08)) {
									table.writeFloat(Float.parseFloat(updateCommandSplit.get(5).trim()));
								} else if (valLength.get(i - 1) == (0x09)) {
									table.writeDouble(Double.parseDouble(updateCommandSplit.get(5).trim()));
								} else if (valLength.get(i - 1) == (0x0B)) {
									table.writeLong(convertStringToDate(updateCommandSplit.get(5).trim()));
								} else if (valLength.get(i - 1) == (0x0A)) {
									table.writeLong(Long.parseLong(updateCommandSplit.get(5).trim()));
								} else if(valLength.get(i - 1)  > 0x0C) {
									String newStr = updateCommandSplit.get(5).replace("'","").replace("\"","").trim();
									int newStrLen = newStr.length();
									int oldStrLen = valLength.get(i - 1) - 0x0C;
									for (int k = 0; k < (oldStrLen - newStrLen); k++) {
										newStr = newStr + "\0";
									}
									table.writeBytes(newStr);
									
								}	
								
							i++;

							System.out.println("Updated successfully");
							break;
						}
						else {
							colCount++;
							if (i == 0) {
								
							}
							else {
								if (valLength.get(i - 1) == 0x06) {
									table.readInt();
								} else if (valLength.get(i - 1) == (0x04)) {
									table.readByte();
								} else if (valLength.get(i - 1) == (0x05)) {
									table.readShort();
								} else if (valLength.get(i - 1) == (0x07)) {
									table.readLong();
								} else if (valLength.get(i - 1) == (0x08)) {
									table.readFloat();
								} else if (valLength.get(i - 1) == (0x09)) {
									table.readDouble();
								} else if (valLength.get(i - 1) == (0x0B)) {
									table.readLong();
								} else if (valLength.get(i - 1) == (0x0A)) {
									table.readLong();
								} else if(valLength.get(i - 1)  > 0x0C) {									
									int length = valLength.get(i - 1) - 0x0C;
									byte[] bytes = new byte[length];
									table.read(bytes, 0, bytes.length);									
								}	
							}
							i++;
						}
					}	
				} 
				else {
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
	public long convertStringToDate(String dateString) {
		String pattern = "MM:dd:yyyy";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		try {
			Date date = format.parse(dateString);
			return date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new Date().getTime();
	}
}
