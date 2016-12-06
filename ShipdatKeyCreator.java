import com.sleepycat.je.*;
import net.sf.jsqlparser.statement.create.table.*;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;


public class ShipdatKeyCreator implements SecondaryKeyCreator{
	String key;
	List<ColumnDefinition> colDefs;
	public ShipdatKeyCreator(String kname,List<ColumnDefinition> coldef){
		key=kname;
		colDefs=coldef;
	}

	public boolean createSecondaryKey(SecondaryDatabase secDb,DatabaseEntry keyEntry,DatabaseEntry dataEntry,DatabaseEntry resultEntry){
		int index=-1;
		List<Object> items=new ArrayList<Object>();
		byte[] data=dataEntry.getData();
		ByteArrayInputStream from=new ByteArrayInputStream(data);
		DataInputStream in=new DataInputStream(from);
		String colname=null;
		String coltype=null;
		try{
		for(int i=0;i<colDefs.size();i++){
			colname=colDefs.get(i).getColumnName();
			coltype=colDefs.get(i).getColDataType().getDataType();
			if(coltype.toLowerCase().equals("char")||coltype.toLowerCase().equals("varchar")||coltype.toLowerCase().equals("string")){
				items.add(in.readUTF());
			}else if(coltype.toLowerCase().equals("int")){
				items.add(in.readLong());
			}else if(coltype.toLowerCase().equals("decimal")){
				items.add(in.readDouble());
			}else if(coltype.toLowerCase().equals("date")){
				items.add(in.readUTF());
			}
			if(colname.toLowerCase().equals(key)){
				index=i;
				break;
			}
		}
		if(index<0)
			return false;
		if(coltype.toLowerCase().equals("char")||coltype.toLowerCase().equals("varchar")||coltype.toLowerCase().equals("string")){
			resultEntry.setData(((String)items.get(index)).getBytes("UTF-8"));
		}else if(coltype.toLowerCase().equals("int")){
			ByteBuffer buf=ByteBuffer.allocate(8);
			buf.putLong((Long)items.get(index));
			resultEntry.setData(buf.array());
		}else if(coltype.toLowerCase().equals("decimal")){
			ByteBuffer buf=ByteBuffer.allocate(8);
			buf.putDouble((Double)items.get(index));
			resultEntry.setData(buf.array());
		}else if(coltype.toLowerCase().equals("date")){
			resultEntry.setData(((String)items.get(index)).getBytes("UTF-8"));
		}
		}catch(IOException e){
			e.printStackTrace();
		}
		return true;
	}
}
