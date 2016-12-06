import java.sql.SQLException;
import java.util.*;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.select.*;
import edu.buffalo.cse562.*;

public class Projection extends RAOperator {

	private List<SelectItem> selectItems;
	private List<Column> tupleSchema;
	private List<Column> childSchema;
	//constructor
	public Projection(){
		selectItems=null;
		tupleSchema=null;
	}
	
	public Projection(List<SelectItem> items,RAOperator op){
		selectItems=items;
		super.setLeftChild(op);
		setTupleSchema();
	}
	
	void setTupleSchema(){
		List<SelectItem> items=selectItems;
		tupleSchema=new ArrayList<Column>();
		childSchema=getChild().getTupleSchema();
		for(int i=0;i<items.size();i++){
			if(items.get(i) instanceof AllColumns){
				tupleSchema.addAll(childSchema);
				break;
			}else if(items.get(i) instanceof AllTableColumns){
				Table tab=((AllTableColumns)items.get(i)).getTable();
				for(int j=0;j<childSchema.size();j++){
					Column col=childSchema.get(j);
					if(col.getTable().getName().equals(tab.getName())){//may have problem
						tupleSchema.add(col);
					}
				}
			}else {//instanceof SelectExpressionItem
				Expression exp=((SelectExpressionItem)items.get(i)).getExpression();
				String alias=((SelectExpressionItem)items.get(i)).getAlias();
				if(exp instanceof Column){
					Column newCol=new Column();
					newCol.setTable(((Column)exp).getTable());
					newCol.setColumnName(((Column)exp).getColumnName());
					if(alias!=null)
						newCol.setColumnName(alias);
					tupleSchema.add(newCol);
				}else if(exp instanceof Function){
					Table empty=new Table();
					String func=((Function)exp).getName().toUpperCase();
					String colName;
					if(func.equals("COUNT"))
					colName=((Function)exp).getName();
					else colName=((Function)exp).getName()+((Function)exp).getParameters().toString();
					Column col=new Column(empty,colName);
					if(alias!=null)
						col.setColumnName(alias);
					tupleSchema.add(col);
				}else{
					Table empty=new Table();
					Column col=new Column(empty,exp.toString());
					if(alias!=null)
						col.setColumnName(alias);
					tupleSchema.add(col);
				}
			}
		}
	}
	//getters and setters
	public List<SelectItem> getSelectItems(){
		return selectItems;
	}
	
	public void setSelectItems(List<SelectItem> items){
		selectItems=items;
		setTupleSchema();
	}
	
	public RAOperator getChild(){
		return getLeftChild();
	}
	
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
	
	//RAIterator methods
	
	public void open(){
		//printTupleSchema(getTupleSchema());
		getChild().open();
	}
	
	public List<LeafValue> getNext(){
		List<LeafValue> oldTuple=getChild().getNext();	
		List<LeafValue> newTuple=new ArrayList<LeafValue>();
		if(oldTuple==null)
			return null;
		//printTuple(oldTuple);
		for(int i=0;i<selectItems.size();i++){
			if(selectItems.get(i) instanceof AllColumns){
				return oldTuple;
			}else if(selectItems.get(i) instanceof AllTableColumns){
				Table tab=((AllTableColumns)selectItems.get(i)).getTable();
				for(int j=0;j<childSchema.size();j++){
					Column col=childSchema.get(j);
					if(col.getTable().getName().equals(tab.getName())){//may have problem
						newTuple.add(oldTuple.get(indexOf(col,childSchema)));
					}
				}
			}else {//instanceof SelectExpressionItem
				Expression exp=((SelectExpressionItem)selectItems.get(i)).getExpression();
				String alias=((SelectExpressionItem)selectItems.get(i)).getAlias();
				if(exp instanceof Column){
					//printColumn((Column)exp);
					//System.out.println(alias);
					//printTupleSchema(childSchema);
					newTuple.add(oldTuple.get(indexOf((Column)exp,childSchema)));
				}else if(exp instanceof Function){
					Table empty=new Table();
					String func=((Function)exp).getName().toUpperCase();
					String colName;
					if(func.equals("COUNT"))
					colName=((Function)exp).getName();
					else colName=((Function)exp).getName()+((Function)exp).getParameters().toString();
					Column col=new Column(empty,colName);
					newTuple.add(oldTuple.get(indexOf(col,childSchema)));
				}else{//for derivative column
					try{
					ExpEval eva=new ExpEval(childSchema,oldTuple);
					LeafValue val=eva.eval(exp);
					newTuple.add(val);
				}catch(SQLException e){
					e.printStackTrace();
				}
			}
			}
		}
		return newTuple;
	}
	
	public void close(){
		getChild().close();
	}
}
