import java.sql.SQLException;
import java.util.*;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.operators.relational.*;

public class Selection extends RAOperator{
	private Expression condition;
	private List<Column> tupleSchema;
	private List<LeafValue> tuple;
	private List<Column> childSchema;
	
	//constructors
	public Selection(){
		condition=null;
	}
	
	public Selection(Expression cond,RAOperator chil){
		condition=cond;
		super.setLeftChild(chil);
		setTupleSchema();
	}
	
	public void setTupleSchema(){
		tupleSchema=new ArrayList<Column>();
		tupleSchema.addAll(getChild().getTupleSchema());
		childSchema=getChild().getTupleSchema();
	}
	//getters and setters
	public Expression  getCondition(){
		return condition;
	}
	public void setCondition(Expression exp){
		condition=exp;
	}
	
	public RAOperator getChild(){
		return getLeftChild();
	}
	
	public void setLeftChild(RAOperator left){
		super.setLeftChild(left);
		setTupleSchema();
	}
	
	public List<Column> getTupleSchema(){
		return tupleSchema;
	}

	public List<Column> getOldSchema(){
		return null;		
	}
	//RAIterator methods
	public void open(){
		getChild().open();
		//printTupleSchema(getTupleSchema());
	}
	
	public List<LeafValue> getNext(){
		while(true){
		tuple=getChild().getNext();
		if(tuple==null)
			return null;
		try{
			//printCondition();
			//RAOperator.printTupleSchema(childSchema);
		ExpEval eval=new ExpEval(childSchema,tuple);
		BooleanValue value=(BooleanValue)eval.eval(condition);
		if(value.getValue())
			return tuple;
		}catch(SQLException e){
			e.printStackTrace();
			return null;
		}
		}
	}
	
	public void close(){
		getChild().close();
	}
	
	//used for debugging
	public void printCondition(){
		if(condition instanceof EqualsTo){
			Column col=(Column)((EqualsTo)condition).getLeftExpression();
			LongValue val=(LongValue)((EqualsTo)condition).getRightExpression();
			System.out.println(col.getWholeColumnName()+'='+val.toString());
		}else{
			List<Column> cols=RATree.getColumnFromCond(getCondition());
			for(int i=0;i<cols.size();i++){
				System.out.print(cols.get(i).getWholeColumnName()+"  ");
			}
			System.out.print('\n');
		}
	}
}
