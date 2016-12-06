/*Product_NLJ is the RAOperator for implementing the block nested loop algorithm for product*/
import java.util.*;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class Product_NLJ extends RAOperator{

	private List<LeafValue> rtuple;
	private int rtupleInd;
	private List<List<LeafValue>> rtable;
	List<Column> tupleSchema;
	
	final int Buffer_size=10000;
	List<List<LeafValue>> lbuffer;
	int buffer_ind;
	RAOperator lchild;
	RAOperator rchild;
	
	public Product_NLJ(){
		tupleSchema=null;
	}
	
	public Product_NLJ(RAOperator left,RAOperator right){
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
		lchild=getLeftChild();
		lchild.open();
		rchild=getRightChild();
		rchild.open();
		lbuffer=new ArrayList<List<LeafValue>>();
		List<LeafValue> tuple;
		
		int index=0;
		while((tuple=lchild.getNext())!=null&&index<Buffer_size){
			lbuffer.add(tuple);
			index++;
		}
		buffer_ind=0;
		rtuple=rchild.getNext();
		///printTupleSchema(getTupleSchema());
		//System.out.println("rtable size="+rtable.size());
	}
	
	public List<LeafValue> getNext(){
		List<LeafValue> newTuple=new ArrayList<LeafValue>();
		List<LeafValue> lold;
		List<LeafValue> tuple;
		if(lbuffer.size()==0)//the left table is exhausted
			return null;
		if(rtuple==null){//all tuples in left table buffer has been paired, read new tuples into the buffer
			// and start to pair with the right table from beginning
			int index=0;
			lbuffer.clear();
			//System.gc();
			while((tuple=lchild.getNext())!=null&&index<Buffer_size){
				lbuffer.add(tuple);
				index++;
			}
			buffer_ind=0;
			rchild.close();
			//System.gc();
			rchild.open();
			rtuple=rchild.getNext();
			return getNext();
		}
		if(buffer_ind>=lbuffer.size()){//go on to pair with the next tuple from the right table
			rtuple=rchild.getNext();
			buffer_ind=0;
			return getNext();
		}
		newTuple.addAll(lbuffer.get(buffer_ind));
		newTuple.addAll(rtuple);
		buffer_ind++;
		return newTuple;
		
	}

	public void close(){
		lchild.close();
		rchild.close();
	}
}
