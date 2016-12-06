import java.util.*;

import java.sql.SQLException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class EquiJoin extends RAOperator{
	private Expression condition;
	private List<Column> tupleSchema;
	private List<LeafValue> ltuple;
	private List<Expression> lexps;
	private List<Expression> rexps;
	private HashMap<String,List<ArrayList<LeafValue>>> hsm;
	private int rindex;
	private List<LeafValue> rtuple;
	private List<ArrayList<LeafValue>> slot;
	private List<LeafValue> newTuple;
 
	public EquiJoin(){
		condition=null;
		tupleSchema=null;
	}
	
	public EquiJoin(Expression cond,RAOperator leftOp,RAOperator rightOp){
		condition=cond;
		setLeftChild(leftOp);
		setRightChild(rightOp);
		tupleSchema=new ArrayList<Column>();
		tupleSchema.addAll(leftOp.getTupleSchema());
		tupleSchema.addAll(rightOp.getTupleSchema());
		lexps=new ArrayList<Expression>();
		rexps=new ArrayList<Expression>();
		List<Expression> exps=RATree.getSeparatedCond(condition);
		for(int i=0;i<exps.size();i++){
			Expression left=((EqualsTo)exps.get(i)).getLeftExpression();
			Expression right=((EqualsTo)exps.get(i)).getRightExpression();
			List<Column> lcols=RATree.getColumnFromCond(left);
			if(RATree.associatedWith(lcols,getLeftChild().getTupleSchema())){
				lexps.add(left);
				rexps.add(right);
			}else{
				lexps.add((Column)right);
				rexps.add((Column)left);
			}
		}
	}
	
	public Expression getCondition(){
		return condition;
	}
	
	public void setCondition(Expression exp){
		condition=exp;
		lexps=new ArrayList<Expression>();
		rexps=new ArrayList<Expression>();
		List<Expression> exps=RATree.getSeparatedCond(condition);
		for(int i=0;i<exps.size();i++){
			Expression left=((EqualsTo)exps.get(i)).getLeftExpression();
			Expression right=((EqualsTo)exps.get(i)).getRightExpression();
			List<Column> lcols=RATree.getColumnFromCond(left);
			if(RATree.associatedWith(lcols,getLeftChild().getTupleSchema())){
				lexps.add(left);
				rexps.add(right);
			}else{
				lexps.add((Column)right);
				rexps.add((Column)left);
			}
		}
	}
	
	public List<Column> getTupleSchema(){
		return tupleSchema;
	}
	
	public List<Column> getOldSchema(){
		return null;
	}
	//implement the RAIterator interface
	public void open(){
		RAOperator left=getLeftChild();
		left.open();
		RAOperator right=getRightChild();
		right.open();
		hsm=new HashMap<String,List<ArrayList<LeafValue>>>();
		while((ltuple=left.getNext())!=null){
			String key="";
			try{
			ExpEval eval=new ExpEval(left.getTupleSchema(),ltuple);
			for(int i=0;i<lexps.size();i++){
				key+=(eval.eval(lexps.get(i))).toString();
			}}catch(SQLException e){
				e.printStackTrace();
			}
			if(!hsm.keySet().contains(key)){
				hsm.put(key, new ArrayList<ArrayList<LeafValue>>());
			}
			List<ArrayList<LeafValue>> cg=hsm.get(key);
			cg.add((ArrayList)ltuple);
		}
		rindex=0;
		rtuple=right.getNext();
		while(rtuple!=null){
			String rkey="";
			try{
			ExpEval eval=new ExpEval(right.getTupleSchema(),rtuple);
			for(int i=0;i<rexps.size();i++){
				rkey+=(eval.eval(rexps.get(i))).toString();
			}}catch(SQLException e){
				e.printStackTrace();
			}
			if((slot=hsm.get(rkey))==null){
				rtuple=right.getNext();
			}else{
				break;
			}
		}
	}
	
	public List<LeafValue> getNext(){
		if(rtuple==null)
			return null;
		if(rindex>=slot.size()){
			rindex=0;
			rtuple=getRightChild().getNext();
			while(rtuple!=null){
				String rkey="";
				try{
				ExpEval eval=new ExpEval(getRightChild().getTupleSchema(),rtuple);
				for(int i=0;i<rexps.size();i++){
					rkey+=(eval.eval(rexps.get(i))).toString();
				}}catch(SQLException e){
					e.printStackTrace();
				}
				if((slot=hsm.get(rkey))==null){
					rtuple=getRightChild().getNext();
				}else{
					break;
				}
			}
			return getNext();
		}else{
			newTuple=new ArrayList<LeafValue>();
			newTuple.addAll(slot.get(rindex++));
			newTuple.addAll(rtuple);
			return newTuple;
		}
	}
	
	public void close(){
		getLeftChild().close();
		getRightChild().close();
	}

}
