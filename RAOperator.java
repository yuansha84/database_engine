import java.util.*;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.select.*;

public abstract class RAOperator implements RAIterator{

	private RAOperator leftChild;
	private RAOperator rightChild;
    private String alias;
    
	public RAOperator(){
		leftChild=rightChild=null;
		alias=null;
	}
	
	//getters and setters
	public RAOperator getLeftChild(){
		return leftChild;
	}
	
	public void setLeftChild(RAOperator left){
		leftChild=left;
	}
	
	public RAOperator getRightChild(){
		return rightChild;
	}
	
	public void setRightChild(RAOperator right){
		rightChild=right;
	}
	
	public String getAlias(){
		return alias;
	}
	
	public void setAlias(String ali){
		alias=ali;
	}
	
	//used for test
	public static void printTupleSchema(List<Column> shc){
		for(int i=0;i<shc.size();i++){
			Column col=shc.get(i);
			System.out.print(col.getWholeColumnName());
			if(i!=shc.size()-1)
				System.out.print(' ');
			else System.out.print('\n');
		}
	}
	
	//used for test
	public static void printTuple(List<LeafValue> shc){
		for(int i=0;i<shc.size();i++){
			LeafValue val=shc.get(i);
			System.out.print(val.toString()+' ');
			if(i==shc.size()-1)
				System.out.println('\n');
			
		}
	}
	
	//used for test
	public static void printColumn(Column col){
		/*if(col.getTable().getName()==null)
			System.out.println(col.getColumnName());
		else System.out.println(col.getWholeColumnName());*/
		String tableName,colName;
		if((tableName=col.getTable().getName())!=null)
			System.out.print("tableName:"+tableName+"  ");
		colName=col.getColumnName();
		System.out.print("colName:"+colName+'\n');
		
	}
	
	//utility method for finding the index of a col in a list of cols
	public static int indexOf(Column col,List<Column> schema){
		int index=-1;
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
	
	public abstract List<Column> getTupleSchema();
	public abstract List<Column> getOldSchema();
}
