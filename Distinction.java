import java.util.*;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.*;

public class Distinction extends RAOperator{

	private List<Column> tupleSchema;
	private HashSet<List<LeafValue>> hs;
	private List<LeafValue> tuple;
	
	public Distinction(RAOperator op){
		super.setLeftChild(op);
		setTupleSchema();
	}
	
	private void setTupleSchema(){
		tupleSchema =getLeftChild().getTupleSchema();
	}
	
	//override
	public void setLeftChild(RAOperator left){
		super.setLeftChild(left);
		setTupleSchema();
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
	
	public void open() {
		getLeftChild().open();
		hs = new HashSet<List<LeafValue>>();
	}

	
	public List<LeafValue> getNext() {
		while(true){
			tuple=getLeftChild().getNext();
		if(tuple==null)
			return null;
		if(!hs.contains(tuple)){
			hs.add(tuple);
			return tuple;
		}
		}
	}


	public void close() {
		getLeftChild().close();
	}

}
