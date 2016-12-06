import java.util.*;

import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;

public class Unionation extends RAOperator{

	private List<Column> tupleSchema;
	private List<LeafValue> ltuple;
	private List<LeafValue> rtuple;
	
	public Unionation(){
		
	}
	public Unionation(RAOperator left,RAOperator right){
		setLeftChild(left);
		setRightChild(right);
		tupleSchema=getLeftChild().getTupleSchema();
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
		getLeftChild().open();
		getRightChild().open();
		ltuple=getLeftChild().getNext();
		rtuple=getRightChild().getNext();
	}
	
	public List<LeafValue> getNext(){
		if(ltuple==null){
			if(rtuple!=null){
				List<LeafValue> tmp=rtuple;
				rtuple=getRightChild().getNext();
				return tmp;
			}else{
				return null;
			}
		}else{
			List<LeafValue> tmp=ltuple;
			ltuple=getLeftChild().getNext();
			return tmp;
		}
	}
	
	public void close(){
		getLeftChild().close();
		getRightChild().close();
	}
}
