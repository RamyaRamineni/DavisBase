package DavisBase;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;






public class insertCommand{

	private String command = null;
	private String dml_insert = "insert into";
		
	public insertCommand(String command) {
		this.command = command;		
	}

	public boolean execute() {
		if(command != null) {
			return parseInsertString();
		} else {
			System.out.println("ERROR: command is empty");
			return false;
		}
	}
	
	@SuppressWarnings({ "null", "deprecation" })
	public boolean parseInsertString() {
		
		boolean valid = false;
		boolean isFileExists = false;
		if(command.startsWith(dml_insert)){
            int valuesIndex = command.indexOf("values");
            if(valuesIndex == -1) {
                System.out.println("Expected VALUES keyword");
                return false;
            }         
            
            int isRow_idPresent = command.indexOf("row_id");
            if(isRow_idPresent > 0) {
                System.out.println("row_id is auto incremented. Please do not give value for row_id.");
                return false;
            }


            String columnOptions = command.substring(0, valuesIndex);
            int openBracketIndex = columnOptions.indexOf("(");

            if(openBracketIndex != -1) {
                //tableName = command.substring(dml_insert.length(), openBracketIndex).trim();
                int closeBracketIndex = command.indexOf(")");
                if(closeBracketIndex == -1) {
                	System.out.println("Expected ')'");
                    return false;
                }              
            }
            String valuesList = command.substring(valuesIndex + "values".length()).trim();
            if(!valuesList.startsWith("(")){
            	System.out.println("Expected '('");
                return false;
            }

            if(!valuesList.endsWith(")")){
            	System.out.println("Expected ')'");
                return false;
            }
            File file = new File("data/catalog/davisbase_columns.tbl");
			if ((file.exists()) && (!file.isDirectory())) {
				isFileExists = true;
			}
            valid = true;
        }

		if(valid && isFileExists) {
			try {
				ArrayList<String> insertCommandSplit = new ArrayList<String>(Arrays.asList(command.split(" ")));
				String tableName = insertCommandSplit.get(2);
				utility util = new utility();
				java.util.List<column> col = util.getTableCols(tableName);
								
				RandomAccessFile tables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
				int rows = 0;				
				long pos = -1L;
				long tablesInitialPointer = tables.getFilePointer();
				long tablesFileLength = tables.length();
				
				tables.read();
				while (tablesInitialPointer < tablesFileLength) {
					tables.read();
					tables.readByte();
					byte length = tables.readByte();
					byte[] bytes = new byte[length];
					tables.read(bytes, 0, bytes.length);
					String tablesTableName = new String(bytes);
					pos = tables.getFilePointer();
					rows = tables.readInt();
					if (tablesTableName.equals(tableName)) {
						rows++;
						break;
					}
				}
				
				String userCommand = command.replace('(', '#').replace(')', ' ').trim();
				userCommand = userCommand.replace("\"","").trim();
				String[] tokens = userCommand.replace("'","").split("#");
				tokens[1] = (rows + "," + tokens[1]);
				String[] values = tokens[2].trim().split(",");
				for (int j = 0; j < values.length; j++) {
					values[j] = values[j].trim();
				}
			
				int recordSize = 0;
				boolean isError = false;
				if ((col.size()-1) == values.length) {//-1 as we dont consider row_id
					
					int i;
					for (int j = 0; j < values.length; j++) {
						i = j + 1;
						if ((((column) col.get(i)).isNotNullable()) || (((column) col.get(i)).isPrimary())) {
							if ((values[j].trim().equals("null"))) {
								isError = true;
							}
							if (isError)
								break;
						}
						if (((column) col.get(i)).getDataType().equals("int")) {
							recordSize += 4;
						} else if (((column) col.get(i)).getDataType().equals("tinyint")) {
							recordSize++;
						} else if (((column) col.get(i)).getDataType().equals("smallint")) {
							recordSize += 2;
						} else if (((column) col.get(i)).getDataType().equals("bigint")) {
							recordSize += 8;
						} else if (((column) col.get(i)).getDataType().equals("real")) {
							recordSize += 4;
						} else if (((column) col.get(i)).getDataType().equals("double")) {
							recordSize += 8;
						} else if (((column) col.get(i)).getDataType().equals("date")) {
							recordSize += 8;
						} else if (((column) col.get(i)).getDataType().equals("datetime")) {
							recordSize += 8;
						} else if (((column) col.get(i)).getDataType().equals("text")) {
							//values[i-1] = values[i-1].trim();
							recordSize += values[i-1].length();
						}
					}
				}
				else {
					System.out.println("ERROR: Number of column names and values provided are not matching");
					tables.close();
					return false;
				}

				if (!isError) {
					tables.seek(pos);
					tables.writeInt(rows);//storing number of rows in tables file next to each table name
					BTree btree = new BTree();
					
					BTree.tableName = "data/user_data/" + tableName + ".tbl";//setting table name and its static
					long pointer = btree.insert(recordSize + col.size());
					if (pointer == 0L) {
						return false;
					}
					RandomAccessFile table = new RandomAccessFile(BTree.tableName, "rw");
					table.seek(pointer);
					table.writeShort(recordSize + col.size());
					table.writeInt(rows);
					table.writeByte(col.size() - 1);
					
					for (int i = 1; i < values.length + 1; i++) {
						if (((column) col.get(i)).getDataType().equals("int")) {
							table.writeByte(0x06);
						} else if (((column) col.get(i)).getDataType().equals("tinyint")) {
							table.writeByte(0x04);
						} else if (((column) col.get(i)).getDataType().equals("smallint")) {
							table.writeByte(0x05);
						} else if (((column) col.get(i)).getDataType().equals("bigint")) {
							table.writeByte(0x07);
						} else if (((column) col.get(i)).getDataType().equals("real")) {
							table.writeByte(0x08);
						} else if (((column) col.get(i)).getDataType().equals("double")) {
							table.writeByte(0x09);
						} else if (((column) col.get(i)).getDataType().equals("date")) {
							table.writeByte(0x0B);
						} else if (((column) col.get(i)).getDataType().equals("datetime")) {
							table.writeByte(0x0A);
						} else if (((column) col.get(i)).getDataType().equals("text")) {							
							table.writeByte(0x0C + values[i-1].length());
						}
					}
					
					for (int i = 1; i < values.length + 1; i++) {
						
						if (((column) col.get(i)).getDataType().equals("int")) {
							if (values[i-1].trim().equals("null")) {								
								table.writeInt(0x02);
							}
							else {							
							table.writeInt(Integer.parseInt(values[i-1]));
							}							
						} else if (((column) col.get(i)).getDataType().equals("tinyint")) {
							if (values[i-1].trim().equals("null")) {
								table.writeByte(0x00);
							}
							else {
							table.writeByte(Byte.parseByte(values[i-1]));
							}
						} else if (((column) col.get(i)).getDataType().equals("smallint")) {
							if (values[i-1].trim().equals("null")) {
								table.writeShort(0x01);
							}
							else {
							table.writeShort(Short.parseShort(values[i-1]));
							}
						} else if (((column) col.get(i)).getDataType().equals("bigint")) {
							table.writeLong(Long.parseLong(values[i-1]));
						} else if (((column) col.get(i)).getDataType().equals("real")) {
							if (values[i-1].trim().equals("null")) {
								table.writeFloat(0x02);
							}
							else {
							table.writeFloat(Float.parseFloat(values[i-1]));
							}
						} else if (((column) col.get(i)).getDataType().equals("double")) {
							if (values[i-1].trim().equals("null")) {
								table.writeDouble(0x03);
							}
							else {
							table.writeDouble(Double.parseDouble(values[i-1]));
							}
						} else if (((column) col.get(i)).getDataType().equals("date")) {
							if (values[i-1].trim().equals("null")) {								
								table.writeLong(0x03);
							}
							else {				
								SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");								
								Date date = dateFormat.parse(values[i-1]);
								long milliseconds = date.getTime();
								table.writeLong(milliseconds);							
								
							}
						} else if (((column) col.get(i)).getDataType().equals("datetime")) {
							if (values[i-1].trim().equals("null")) {
								table.writeLong(0x03);
							}
							else {
								SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");								
								Date date = dateFormat.parse(values[i-1]);
								long milliseconds = date.getTime();
								table.writeLong(milliseconds);					
							}
						} else if (((column) col.get(i)).getDataType().equals("text")) {							
							table.writeBytes(values[i-1]);
						}
					}
					table.close();
					System.out.println("Record is inserted Successfully");
					tables.close();
					
					
				} else {					
					System.out.println("Nullable Field can't be null");
					tables.close();
					
				}
				
			} catch (Exception e) {
				System.out.println("ERROR");
				
			}
		return true;
		}
		else {
			System.out.println("ERROR");
			return false;
		}
		
	}
	private long parse(SimpleDateFormat date1) {
		// TODO Auto-generated method stub
		return 0;
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
		
