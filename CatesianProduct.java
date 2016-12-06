import java.util.*;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class CatesianProduct extends RAOperator{

	private List<LeafValue> ltuple;
	private int rtupleInd;
	private List<List<LeafValue>> rtable;
	List<Column> tupleSchema;
	
	public CatesianProduct(){
		tupleSchema=null;
	}
	
	public CatesianProduct(RAOperator left,RAOperator right){
		super.setLeftChild(left);
		super.setRightChild(right);
		tupleSchema=new ArrayList<Column>();
		tupleSchema.addAll(left.getTupleSchema());
		tupleSchema.addAll(right.getTupleSchema());
	}
	
	public List<Column> getTupleSchema(){
		return tupleSchema;
	}
	
	//overide
	public void setLeftChild(RAOperator left){
		super.setLeftChild(left);
		tupleSchema=new ArrayList<Column>();
		tupleSchema.addAll(getLeftChild().getTupleSchema());
		tupleSchema.addAll(getRightChild().getTupleSchema());
	}
	
	//override
	public void setRightChild(RAOperator right){
		super.setRightChild(right);
		tupleSchema=new ArrayList<Column>();
		tupleSchema.addAll(getLeftChild().getTupleSchema());
		tupleSchema.addAll(getRightChild().getTupleSchema());
	}
	
	public List<Column> getOldSchema(){
		return null;
	}
	public void open(){
		RAOperator lchild=getLeftChild();
		lchild.open();
		RAOperator rchild=getRightChild();
		rchild.open();
		rtable=new ArrayList<List<LeafValue>>();
		List<LeafValue> rt;
		while((rt=rchild.getNext())!=null){
			rtable.add(rt);
		}
		ltuple=lchild.getNext();
		rtupleInd=0;
		///printTupleSchema(getTupleSchema());
		//System.out.println("rtable size="+rtable.size());
	}
	
	public List<LeafValue> getNext(){
		List<LeafValue> newTuple=new ArrayList<LeafValue>();
		List<LeafValue> lold;
		if(ltuple==null)//over the left table
			return null;
		if(rtupleInd>=rtable.size()){
			ltuple=getLeftChild().getNext();
			rtupleInd=0;
			return getNext();
		}else{
			newTuple.addAll(ltuple);
			newTuple.addAll(rtable.get(rtupleInd));
			rtupleInd++;
			//printNextTuple(newTuple);
			return newTuple;
		}
		
	}
	
	/* a test routine
	public void printNextTuple(List<LeafValue> tup){
		for(int i=0;i<tup.size();i++){
			System.out.print(((LongValue)tup.get(i)).getValue());
			if(i<tup.size()-1)
				System.out.print(' ');
			else System.out.print('\n');
		}
	}
	*/
	public void close(){
		getLeftChild().close();
		getRightChild().close();
	}
}
