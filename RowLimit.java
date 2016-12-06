import java.util.*;

import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;

public class RowLimit extends RAOperator{

	private long  rowNum;
	private List<Column> tupleSchema;
	private int index;
	private List<LeafValue> tuple;
	
	public RowLimit(long rows,RAOperator op){
		rowNum=rows;
		setLeftChild(op);
		tupleSchema=getChild().getTupleSchema();
	}
	
	public long getRowNum(){
		return rowNum;	
	}
	
	public void setRowNum(long rows){
		rowNum=rows;
	}
	
	public RAOperator getChild(){
		return getLeftChild();
	}
	
	
	public List<Column> getTupleSchema(){
		String alias=getAlias();
		if(alias==null)
		return tupleSchema;
		else{
			List<Column> newSchema=new ArrayList<Column>();
			Table tab=new Table();
			tab.setName(alias);
			for(int i=0;i<tupleSchema.size();i++){
				Column col=new Column();
				col.setColumnName(tupleSchema.get(i).getColumnName());
				col.setTable(tab);
				newSchema.add(col);
			}
			return newSchema;
		}
		
	}
	
	public List<Column> getOldSchema(){
		return tupleSchema;
	}
	
	public void open(){
		getChild().open();
		index=0;
		tuple=getChild().getNext();
	}
	
	public List<LeafValue> getNext(){
		if(index>=rowNum||tuple==null)
			return null;
		index++;
		List<LeafValue> values=tuple;
		tuple=getChild().getNext();
		return values;
	}
	
	public void close(){
		getChild().close();
	}
}
