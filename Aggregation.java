import java.sql.SQLException;
import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.select.*;

public class Aggregation extends RAOperator{

	private List<Column> groupColumns;
	private List<Function> functions;
	//private RAOperator child;
	private List<Column> tupleSchema;
	private List<Column> childSchema;
	private List<LeafValue> oldTuple;
	private List<List<LeafValue>> newTable;
	//private HashMap<String,List<ArrayList<LeafValue>>> groups;
	private HashMap<String,List<LeafValue>> groups;
	private List<LeafValue> newTuple;
	private int newTupleIndex;
	
	public Aggregation(){
		groupColumns=null;
		functions=null;
	}
	
	public Aggregation(List<Column> groupBy,List<Function> fun,RAOperator op){
		groupColumns=groupBy;
		functions=fun;
		super.setLeftChild(op);
		setTupleSchema();
	}
	
	private void setTupleSchema(){
		childSchema=getLeftChild().getTupleSchema();
		tupleSchema=new ArrayList<Column>();
		if(groupColumns!=null)
		tupleSchema.addAll(getLeftChild().getTupleSchema());
		for(int i=0;i<functions.size();i++){
			Table empty=new Table();
			String func=functions.get(i).getName().toUpperCase();
			String colName;
			if(func.equals("COUNT"))
			colName=functions.get(i).getName();
			else colName=functions.get(i).getName()+functions.get(i).getParameters().toString();
			Column col=new Column(empty,colName);
			tupleSchema.add(col);
		}
	}
	
	//override
	public void setLeftChild(RAOperator left){
		super.setLeftChild(left);
		setTupleSchema();
	}
	//getters and setters
	public List<Column> getGroupColumns(){
		return groupColumns;
	}
	public void setGroupColumns(List<Column> cols){
		groupColumns=cols;
		setTupleSchema();
	}
	
	public List<Function> getFunction(){
		return functions;
	}
	
	public void setFunction(List<Function> fun){
		functions=fun;
		setTupleSchema();
	}
	

	public List<Column> getTupleSchema(){
		return tupleSchema;		
	}
	
	public List<Column> getOldSchema(){
		return null;		
	}
	
	
	public void open(){
		RAOperator child=getLeftChild();
		child.open();
		groups=new HashMap<String,List<LeafValue>>();
		newTable=new ArrayList<List<LeafValue>>();
		if(groupColumns==null){//aggregate on the whole table
			List<List<LeafValue>> tab=new ArrayList<List<LeafValue>>();
			while((oldTuple=child.getNext())!=null){
				if(tab.size()==0)
					tab.add(getAggrFields(oldTuple));
				else{
					List<LeafValue> gtuple=tab.get(0);
					aggreOn(gtuple,oldTuple,0);
				}
				
			}
			newTuple=formGroupTuple(tab.get(0),0);
			newTable.add(newTuple);
		}else{//divide into groups and aggregate respectively
			int start=childSchema.size();
			List<Integer> gind=new ArrayList<Integer>();
			while((oldTuple=child.getNext())!=null){
				//form key for group attributes
				String key="";
				for(int i=0;i<groupColumns.size();i++){
					LeafValue val=oldTuple.get(indexOf(groupColumns.get(i),childSchema));
					key+=val.toString();
				}
				if(!groups.keySet().contains(key))
					groups.put(key, new ArrayList<LeafValue>());
				List<LeafValue> cg=groups.get(key);
				if(cg.size()==0){
					cg.addAll(oldTuple);
					cg.addAll(getAggrFields(oldTuple));
				}else{
					aggreOn(cg,oldTuple,start);
				}
			}
		Set<String> keySet=groups.keySet();
		Iterator iter=keySet.iterator();
		int aggStart=childSchema.size();
		while(iter.hasNext()){
			String key=(String)iter.next();
			List<LeafValue> curg=groups.get(key);
			newTuple=formGroupTuple(curg,aggStart);
			newTable.add(newTuple);
		}
		//not used anymore, reallocate the space
		groups=null;
		//System.gc();
		}
		newTupleIndex=0;
		
	}
	
	private List<LeafValue> formGroupTuple(List<LeafValue> tuple,int start){
		List<LeafValue> newTuple=new ArrayList<LeafValue>();
		for(int i=0;i<start;i++)
			newTuple.add(tuple.get(i));
		for(int i=0,j=0;i<functions.size();i++,j++){
			Function func=functions.get(i);
			if(func.getName().toUpperCase().equals("COUNT")||func.getName().toUpperCase().equals("MIN")||func.getName().toUpperCase().equals("MAX")||func.getName().toUpperCase().equals("SUM")){
				newTuple.add(tuple.get(j+start));
			}else if(func.getName().toUpperCase().equals("AVG")){
				LeafValue count=tuple.get(j+start);
				j++;
				LeafValue sum=tuple.get(j+start);
				if(sum instanceof LongValue){
					newTuple.add(new LongValue(((LongValue)sum).getValue()/((LongValue)count).getValue()));
				}else{
				newTuple.add(new DoubleValue(((DoubleValue)sum).getValue()/((LongValue)count).getValue()));
				}
			}
		}
		return newTuple;
	}
	
	private List<LeafValue> getAggrFields(List<LeafValue> tuple){
		List<LeafValue> rs=new ArrayList<LeafValue>();
		ExpEval eval=new ExpEval(childSchema,tuple);
		for(int i=0;i<functions.size();i++){
			Function func=functions.get(i);
			if(func.getName().toUpperCase().equals("COUNT")){
				rs.add(new LongValue(1));
			}else{
				try{
				Object obj=func.getParameters().getExpressions().get(0);
				LeafValue val;
				val=eval.eval((Expression)obj);
				if(func.getName().toUpperCase().equals("MIN")||func.getName().toUpperCase().equals("MAX")||func.getName().toUpperCase().equals("SUM")){
				rs.add(val);
				}else if(func.getName().toUpperCase().equals("AVG")){
					rs.add(new LongValue(1));
					rs.add(val);
				}else{
					//leave it here
				}
				}catch(SQLException e){
					e.printStackTrace();
				}
			}
		}
		return rs;
	}
	
	//aggregate tuple onto the group tuple cg
	private void aggreOn(List<LeafValue> cg,List<LeafValue> oldTuple,int start){
		for(int i=0,j=0;i<functions.size();i++,j++){
			Function funct=functions.get(i);
			ExpEval eval=new ExpEval(childSchema,oldTuple);
			if(funct.getName().toUpperCase().equals("COUNT")){
				LongValue val=new LongValue(((LongValue)cg.get(j+start)).getValue()+1);
				cg.set(j+start, val);
			}else{
				//evaluate the function argument expression on oldTuple
				try{
				Object obj=funct.getParameters().getExpressions().get(0);
				LeafValue val;
				val=eval.eval((Expression)obj);
				LeafValue pval=cg.get(j+start);
				if(funct.getName().toUpperCase().equals("MIN")){
					if(val instanceof LongValue){
						if(((LongValue)pval).getValue()>((LongValue)val).getValue())
							cg.set(j+start,val);
					}else if(val instanceof DoubleValue){
						if(((DoubleValue)pval).getValue()>((DoubleValue)val).getValue())
							cg.set(j+start,val);
					}else{
						
					}
				}else if(funct.getName().toUpperCase().equals("MAX")){
					if(val instanceof LongValue){
						if(((LongValue)pval).getValue()<((LongValue)val).getValue())
							cg.set(j+start,val);
					}else if(val instanceof DoubleValue){
						if(((DoubleValue)pval).getValue()<((DoubleValue)val).getValue())
							cg.set(j+start,val);
					}else{
						
					}
				}else if(funct.getName().toUpperCase().equals("SUM")){
					if(val instanceof LongValue){
						cg.set(j+start,new LongValue(((LongValue)val).getValue()+((LongValue)pval).getValue()));
			
					}else if(val instanceof DoubleValue){
						cg.set(j+start,new DoubleValue(((DoubleValue)val).getValue()+((DoubleValue)pval).getValue()));
					}else{
						
					}
				}else if(funct.getName().toUpperCase().equals("AVG")){
					cg.set(j+start,new LongValue(1+((LongValue)pval).getValue()));
					j++;
					pval=cg.get(j+start);
					if(val instanceof LongValue){
						cg.set(j+start,new LongValue(((LongValue)val).getValue()+((LongValue)pval).getValue()));
			
					}else if(val instanceof DoubleValue){
						cg.set(j+start,new DoubleValue(((DoubleValue)val).getValue()+((DoubleValue)pval).getValue()));
					}else{
						
					}
				}else{

				}
			}catch(SQLException e){
				e.printStackTrace();
			}
			}
		}
	}
	
	
	public List<LeafValue> getNext(){
		//printTupleSchema(getTupleSchema());
		if(newTupleIndex>=newTable.size())
			return null;
		List<LeafValue> cur=newTable.get(newTupleIndex);
		newTupleIndex++;
		return cur;
		
	}
	
	public void close(){
		getLeftChild().close();
	}
}

/*
import java.sql.SQLException;
import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.select.*;

public class Aggregation extends RAOperator{

	private List<Column> groupColumns;
	private List<Function> functions;
	private RAOperator child;
	private List<Column> tupleSchema;
	private List<Column> childSchema;
	private List<LeafValue> oldTuple;
	private List<ArrayList<LeafValue>> newTable;
	private HashMap<String,List<ArrayList<LeafValue>>> groups;
	private ArrayList<LeafValue> newTuple;
	private int newTupleIndex;
	
	public Aggregation(){
		groupColumns=null;
		functions=null;
		child=null;
	}
	public Aggregation(List<Column> groupBy,List<Function> fun,RAOperator op){
		groupColumns=groupBy;
		functions=fun;
		super.setLeftChild(op);
		setTupleSchema();
	}
	
	private void setTupleSchema(){
		childSchema=getLeftChild().getTupleSchema();
		tupleSchema=new ArrayList<Column>();
		if(groupColumns!=null)
		tupleSchema.addAll(getLeftChild().getTupleSchema());
		for(int i=0;i<functions.size();i++){
			Table empty=new Table();
			String func=functions.get(i).getName().toUpperCase();
			String colName;
			if(func.equals("COUNT"))
			colName=functions.get(i).getName();
			else colName=functions.get(i).getName()+functions.get(i).getParameters().toString();
			Column col=new Column(empty,colName);
			tupleSchema.add(col);
		}
	}
	
	//override
	public void setLeftChild(RAOperator left){
		super.setLeftChild(left);
		setTupleSchema();
	}
	//getters and setters
	public List<Column> getGroupColumns(){
		return groupColumns;
	}
	public void setGroupColumns(List<Column> cols){
		groupColumns=cols;
		setTupleSchema();
	}
	
	public List<Function> getFunction(){
		return functions;
	}
	
	public void setFunction(List<Function> fun){
		functions=fun;
		setTupleSchema();
	}
	

	public List<Column> getTupleSchema(){
		return tupleSchema;		
	}
	
	public List<Column> getOldSchema(){
		return null;		
	}

	private int indexOf(Column col,List<Column> schema,List<LeafValue> values){
		int index=-1;
		
		//printTupleSchema(schema);
		if(col.getTable().getName()==null){
			for(int i=0;i<schema.size();i++){
				if(col.getColumnName().equals(schema.get(i).getColumnName())){
					index=i;
					break;
				}
			}
		}else{
			for(int i=0;i<schema.size();i++){
				if(col.getWholeColumnName().equals(schema.get(i).getWholeColumnName())){
					index=i;
					break;
		}
			}
		}
		return index;
	}
	
	public void open(){
		//printTupleSchema(getTupleSchema());
		RAOperator child=getLeftChild();
		child.open();
		groups=new HashMap<String,List<ArrayList<LeafValue>>>();
		newTable=new ArrayList<ArrayList<LeafValue>>();
		if(groupColumns==null){//aggregate on the whole table
			List<ArrayList<LeafValue>> tab=new ArrayList<ArrayList<LeafValue>>();
			while((oldTuple=child.getNext())!=null){
				tab.add((ArrayList<LeafValue>)oldTuple);
			}
			newTuple=formNewGroupTuple(tab);
			newTable.add(newTuple);
		}else{
		while((oldTuple=child.getNext())!=null){
			String key="";
			for(int i=0;i<groupColumns.size();i++){
				LeafValue val=oldTuple.get(indexOf(groupColumns.get(i),childSchema,oldTuple));
				key+=val.toString();
			}
			if(!groups.keySet().contains(key))
				groups.put(key, new ArrayList<ArrayList<LeafValue>>());
			List<ArrayList<LeafValue>> cg=groups.get(key);
			cg.add((ArrayList)oldTuple);
		}
		Set<String> keySet=groups.keySet();
		Iterator iter=keySet.iterator();
		while(iter.hasNext()){
			String key=(String)iter.next();
			List<ArrayList<LeafValue>> curg=groups.get(key);
			newTuple=formNewGroupTuple(curg);
			newTable.add(newTuple);
		}
		}
		newTupleIndex=0;
		
	}
	
	private ArrayList<LeafValue> formNewGroupTuple(List<ArrayList<LeafValue>> curg){
		ArrayList<LeafValue> tup=new ArrayList<LeafValue>();
		if(groupColumns!=null)
		tup.addAll(curg.get(0));
		for(int i=0;i<functions.size();i++){
			tup.add(aggrBy(functions.get(i), curg));
		}
		return tup;
	}
	
	private LeafValue aggrBy(Function func,List<ArrayList<LeafValue>> curg){
		String funcName=func.getName().toUpperCase();
		if(funcName.equals("COUNT")){
			return new LongValue(curg.size());
		}
		ArrayList<LeafValue> values=new ArrayList<LeafValue>();
		for(int i=0;i<curg.size();i++){
			ExpEval eva=new ExpEval(childSchema,curg.get(i));
			Object obj=func.getParameters().getExpressions().get(0);
			if(obj instanceof AllColumns){
				return new LongValue(curg.size());
			}else if(obj instanceof Expression){
				try{
				values.add(eva.eval((Expression)obj));	
				}catch(SQLException e){
					e.printStackTrace();
				}
			}
		}
		if(values.get(0) instanceof LongValue){
			if(funcName.equals("MAX"))
				return maxOnLong(values);
			else if(funcName.equals("MIN"))
				return minOnLong( values);
			else if(funcName.equals("SUM"))
				return sumOnLong(values);
			else return avgOnLong(values);
		}else if(values.get(0) instanceof DoubleValue){
			if(funcName.equals("MAX"))
				return maxOnDoub(values);
			else if(funcName.equals("MIN"))
				return minOnDoub( values);
			else if(funcName.equals("SUM"))
				return sumOnDoub(values);
			else return avgOnDoub(values);
		}else{
			return null;
		}
		
	}
	
	private LeafValue maxOnLong(List<LeafValue> values ){
		Long result=((LongValue)values.get(0)).getValue();
		for(int i=1;i<values.size();i++){
			Long cur=((LongValue)values.get(i)).getValue();
			if(cur>result)
				result=cur;
		}
		return new LongValue(result);
	}
	
	private LeafValue minOnLong(List<LeafValue> values ){
		Long result=((LongValue)values.get(0)).getValue();
		for(int i=1;i<values.size();i++){
			Long cur=((LongValue)values.get(i)).getValue();
			if(cur<result)
				result=cur;
		}
		return new LongValue(result);
	}
	
	private LeafValue sumOnLong(List<LeafValue> values ){
		Long result=((LongValue)values.get(0)).getValue();
		for(int i=1;i<values.size();i++){
			Long cur=((LongValue)values.get(i)).getValue();
			result+=cur;
		}
		return new LongValue(result);
	}
	
	private LeafValue avgOnLong(List<LeafValue> values ){
		Long sum=((LongValue)values.get(0)).getValue();
		for(int i=1;i<values.size();i++){
			Long cur=((LongValue)values.get(i)).getValue();
			sum+=cur;
		}
		return new LongValue(sum/values.size());
	}
	
	private LeafValue maxOnDoub(List<LeafValue> values ){
		Double result=((DoubleValue)values.get(0)).getValue();
		for(int i=1;i<values.size();i++){
			Double cur=((DoubleValue)values.get(i)).getValue();
			if(cur>result)
				result=cur;
		}
		return new DoubleValue(result);
	}
	
	private LeafValue minOnDoub(List<LeafValue> values ){
		Double result=((DoubleValue)values.get(0)).getValue();
		for(int i=1;i<values.size();i++){
			Double cur=((DoubleValue)values.get(i)).getValue();
			if(cur<result)
				result=cur;
		}
		return new DoubleValue(result);
	}
	
	private LeafValue sumOnDoub(List<LeafValue> values ){
		Double result=((DoubleValue)values.get(0)).getValue();
		for(int i=1;i<values.size();i++){
			Double cur=((DoubleValue)values.get(i)).getValue();
			result+=cur;
		}
		return new DoubleValue(result);
	}
	
	private LeafValue avgOnDoub(List<LeafValue> values ){
		Double sum=((DoubleValue)values.get(0)).getValue();
		for(int i=1;i<values.size();i++){
			Double cur=((DoubleValue)values.get(i)).getValue();
			sum+=cur;
		}
		return new DoubleValue(sum/values.size());
	}
	
	public List<LeafValue> getNext(){
		if(newTupleIndex>=newTable.size())
			return null;
		List<LeafValue> cur=newTable.get(newTupleIndex);
		newTupleIndex++;
		return cur;
		
	}
	
	public void close(){
		getLeftChild().close();
	}
}*/