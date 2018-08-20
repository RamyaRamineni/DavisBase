package DavisBase;

import java.io.IOException;
import java.io.RandomAccessFile;



public class BTree {
	
	static Long currentPointer = null;
	static String tableName = "";
	static long pageSize = 512L;
	
	public int getRowId() {
		
		try {
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			long tableLength = table.length();
			if (tableLength > 0) {
				table.seek(1);
				int id = table.read();
				table.close();
				return id;
			}
			table.close();
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return 0;
	}	public long insert(int recordSize) {
		try {
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			long tableLength = table.length();
			long tableIndex = table.getFilePointer();
			long pageStart = 0L;
			long pageEnd = pageSize;
			int recCount;
			if (tableLength == 0L) {				
				table.seek(0);
				table.writeByte(0x05);
				table.writeShort(1);//number of children
				table.writeByte(1);//key - number of records
				table.writeLong(pageSize);//value - address	
				table.close();
				pageStart = pageSize;
				pageEnd = pageSize * 2;
				writeLeafHeader(pageStart, pageEnd, recordSize, -1);
				return currentPointer.longValue();
			}
			else {
							}
			table.seek(pageSize);
			int pageType = table.readByte();
			int cells = table.readByte();
			long startPointer = table.readShort();
			int rightPointer = table.readInt();
			while (rightPointer != -1) {//-1 represents leaf node
				table.seek(rightPointer);
				pageType = table.readByte();
				cells = table.readByte();
				startPointer = table.readShort();//record start location
				rightPointer = table.readInt();
			}/*
			Long lastRecordLocation = 512L;
			for (int i = 0; i < cells; i++) {
				lastRecordLocation = (long) table.readShort();				
			}*/

			if ((pageType == 13) && (rightPointer == -1)) {
				tableIndex = table.getFilePointer();
				
				pageStart = tableIndex - 8L;
				
				pageEnd = pageStart + pageSize;
				
				cells++;
				
				if ((startPointer - recordSize - 4 - 2) > (pageStart + (cells * 2) + 8L)) {//there is enough place in the file to insert new record
					
					updateLeafHeader(pageStart, pageEnd, recordSize, rightPointer);
					table.seek(1);
					int children = table.readShort();
					table.seek(3 + (9*(children - 1)));
					int lastRecord = table.readByte();
					table.seek(3 + (9*(children - 1)));
					table.writeByte(lastRecord + 1);//key - number of records

				} else {//go to new page
					table.seek(1);
					int children = table.readShort() + 1;
					table.seek(1);
					table.writeShort(children);//number of children
					int currPoi = (int) table.getFilePointer();
					table.seek(currPoi + (9*(children - 2)));
					int lastRecord = table.readByte();
					//table.seek(currPoi + (9*(children - 1)));
					table.readLong();
					table.writeByte(lastRecord + 1);//key - number of records
					table.writeLong(pageStart + pageSize);//value - address	
					updateRightPointerOfLeafHeader(pageStart, pageEnd, recordSize, (int) (pageStart + pageSize));
					pageStart = pageStart + pageSize;
					pageEnd = pageEnd + pageSize;
					rightPointer = -1;					
					writeLeafHeader(pageStart, pageEnd, recordSize, rightPointer);					
				}
			}
			
			table.close();
			return currentPointer.longValue();
		} catch (Exception e) {
			System.out.println("ERROR");			
		}
		return 0L;
	}

	public boolean writeLeafHeader(long pageStart, long pageEnd, int recordSize, int rightPointer) {
		try {
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			table.seek(pageStart);
			table.writeByte(0x0d);//leaf node
			table.writeByte(1);
			table.writeShort((int) (pageEnd - recordSize - 4L - 2L));
			table.writeInt(rightPointer);//for no right child
			table.writeShort((int) (pageEnd - recordSize - 4L - 2L));//location of start of the record
			currentPointer = Long.valueOf(pageEnd - recordSize)- 4L - 2L;//4 bytes for trow_id and 2 bytes for record size
			table.close();
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	public boolean updateLeafHeader(long pageStart, long pageEnd, int recordSize, int rightPointer) {
		try {
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			//table.seek(pageStart);
			//table.readByte();

			table.seek(pageStart + 1);
			//table.readByte();
			
			int cells = table.readByte() + 1;
			table.seek(pageStart + 1);
			table.writeByte(cells);
			int oldCellAddress = table.readShort();
			int newCellAddress = oldCellAddress - recordSize - 4 - 2;
			table.seek(pageStart + 2);
			table.writeShort(newCellAddress);
			table.readInt();
			long currPointer = table.getFilePointer();
			table.seek( currPointer + (2 * (cells - 1)));			
			
			//table.seek(2);
			table.writeShort(newCellAddress);			
			currentPointer = Long.valueOf(newCellAddress);
			table.close();
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	public boolean updateRightPointerOfLeafHeader(long pageStart, long pageEnd, int recordSize, int rightPointer) {
		try {
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			table.seek(pageStart);
			table.readByte();
			table.readByte();
			table.readShort();
			table.writeInt(rightPointer);
			table.close();
			return true;
		} catch (Exception e) {
		}
		return false;
	}
}
