
import java.util.*;
import java.io.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.expression.*;

public class InputTable extends RAOperator {
	private Table table;
	private String inputFile;
	private BufferedReader bufferReader;
	private HashMap<String,CreateTable> tableMap;
	private File dir;
	private File dbDir;
	private List<ColDataType> colTypes;
	private List<Column> tupleSchema;

	public InputTable(Table crt,HashMap<String,CreateTable> tables,File dataDir,File dbdir){
		table=crt;
		inputFile=table.getName().toUpperCase()+".dat";
		tableMap=tables;
		dir=dataDir;
		String tableName=crt.getName().toUpperCase();
		CreateTable ct=tableMap.get(tableName);
		tupleSchema=new ArrayList<Column>();
		List<ColumnDefinition> colDef=ct.getColumnDefinitions();
		//set new table name
		Table ntab=new Table();
		if(table.getAlias()!=null)
			ntab.setName(table.getAlias());
		else ntab.setName(table.getName());
		
		for(int i=0;i<colDef.size();i++){
			Column col=new Column();
			col.setTable(ntab);
			col.setColumnName(colDef.get(i).getColumnName());
		    tupleSchema.add(col);
		}
	}
	
	public Table getTable(){
		return table;
	}
	public void setTable(Table crt){
		table=crt;
		inputFile=table.getName()+".dat";
		tupleSchema=new ArrayList<Column>();
		CreateTable ct=tableMap.get(table.getName());
		List<ColumnDefinition> colDef=ct.getColumnDefinitions();
		Table ntab=new Table();
		if(table.getAlias()!=null)
			ntab.setName(table.getAlias());
		else ntab.setName(table.getName());
		for(int i=0;i<colDef.size();i++){
			Column col=new Column();
			col.setTable(ntab);
			col.setColumnName(colDef.get(i).getColumnName());
		    tupleSchema.add(col);
		}
		colTypes=new ArrayList<ColDataType>();
		for(int i=0;i<colDef.size();i++){
			colTypes.add(colDef.get(i).getColDataType());
		}
	}
	
	public List<Column> getTupleSchema(){
		return tupleSchema;
	}
	
	public List<Column> getOldSchema(){
		return null;		
	}
	//RAIterator methods
	
	public void open(){
		try{
		File data=new File(dir,inputFile);
		bufferReader=new BufferedReader(new FileReader(data));	
		}catch(IOException e){
			e.printStackTrace();
		}
		//printTupleSchema(getTupleSchema());
	}
	
	public List<LeafValue> getNext(){
		List<LeafValue> row=new ArrayList<LeafValue>();
		try{
		String line=bufferReader.readLine();
		if(line==null)
			return null;
		else{
			String[] items=line.split("\\|");
			for(int i=0;i<colTypes.size();i++){
				
				LeafValue value=parseItem(items[i],colTypes.get(i).getDataType());
				row.add(value);
			}
			/*for(int i=0;i<row.size();i++){
				printLeafValue(row.get(i));
				if(i<row.size()-1)
					System.out.print(' ');
				else System.out.print('\n');
			}*/
			return row;
		}
		}catch(IOException e){
			e.printStackTrace();
			return null;
		}
	}
	/*
	public void printLeafValue(LeafValue val){
		if(val instanceof StringValue)
			System.out.print(((StringValue)val).getValue());
		else if(val instanceof LongValue)
			System.out.print(((LongValue)val).getValue());
		else if(val instanceof DoubleValue)
		System.out.print(((DoubleValue)val).getValue());
		else if(val instanceof DateValue)
			System.out.print(((DateValue)val).getValue());
		else{
			
		}
	}
	*/
	public void close(){
		try{
		bufferReader.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public LeafValue parseItem(String item,String dataType){
		LeafValue leaf;
		if(dataType.toLowerCase().equals("char")||dataType.toLowerCase().equals("varchar")||dataType.toLowerCase().equals("string")){
			return new StringValue('\''+item+'\'');
		}
		else if(dataType.toLowerCase().equals("int"))
			return new LongValue(item);
		else if(dataType.toLowerCase().equals("decimal"))
			return new DoubleValue(item);
		else if(dataType.toLowerCase().equals("date"))
			return new DateValue(' '+item+' ');
		else return null;
	}
}
