import java.util.*;
import java.lang.*;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;

public class OrderBy extends RAOperator{

	private List<OrderByElement> orderByElements;
	private List<Column> tupleSchema;
	Comparator<List<LeafValue>> comp = null;
	private List<List<LeafValue>> resultTable;
	Iterator<List<LeafValue>> Iterator;
	
	public OrderBy(List<OrderByElement> eles,RAOperator op){
		orderByElements=eles;
		super.setLeftChild(op);
		setTupleSchema();
	}
	
	private void setTupleSchema(){
		tupleSchema = getLeftChild().getTupleSchema();
	}
	
	public List<OrderByElement> getOrderByElements(){
		return orderByElements;
	}

	public void setOrderByElements(List<OrderByElement> eles){
		orderByElements=eles;
	}
	
	public RAOperator getChild(){
		return getLeftChild();
	}
	

	public void setLeftChild(RAOperator left){
		super.setLeftChild(left);
		setTupleSchema();
	}
	@Override
	public void open() {
		// TODO Auto-generated method stub
		RAOperator child=getChild();
		child.open();
		resultTable=new ArrayList<List<LeafValue>>();
		List<LeafValue> tuple;
		while((tuple=child.getNext())!=null){
			resultTable.add(tuple);
			//RAOperator.printTuple(tuple);
		}
		comp=new Comparator<List<LeafValue>>(){
			public int compare(List<LeafValue> t1,List<LeafValue> t2){
				for(int i=0;i<orderByElements.size();i++){
					int ind=indexOf((Column)orderByElements.get(i).getExpression(),tupleSchema);
					boolean isAsc=orderByElements.get(i).isAsc();
					//if(isAsc){
					//System.out.println("asc order");}
					//else System.out.println("desc order");
					int cr=compareLeaf(t1.get(ind),t2.get(ind),isAsc);
					if(cr!=0)
						return cr;
				}
				return 0;
			}};
		Collections.sort(resultTable,comp);
		Iterator= resultTable.iterator();
	}

	public static int compareLeaf(LeafValue v1,LeafValue v2,boolean asc){
		if(v1 instanceof StringValue){
			if(asc)
				return ((StringValue)v1).getValue().compareTo(((StringValue)v2).getValue());
			else
				return ((StringValue)v2).getValue().compareTo(((StringValue)v1).getValue());
		}
		if(v1 instanceof DateValue){
			if(asc)
				return ((DateValue)v1).getDate()-((DateValue)v2).getDate();
			else
				return ((DateValue)v2).getDate()-((DateValue)v1).getDate();
		}
		if(v1 instanceof LongValue){
			if(asc){
				Long diff=((LongValue)v1).getValue()-((LongValue)v2).getValue();
				if(diff<0)
					return -1;
				else if(diff>0)
					return 1;
				else return 0;
			}
				
			else{
				Long diff=((LongValue)v2).getValue()-((LongValue)v1).getValue();
				if(diff<0)
					return -1;
				else if(diff>0)
					return 1;
				else return 0;
			}
		}
		if(v1 instanceof DoubleValue){
			if(asc){
				Double diff=((DoubleValue)v1).getValue()-((DoubleValue)v2).getValue();
				if(diff<0)
					return -1;
				else if(diff>0)
					return 1;
				else return 0;
			}
			else{
				Double diff=((DoubleValue)v2).getValue()-((DoubleValue)v1).getValue();
				if(diff<0)
					return -1;
				else if(diff>0)
					return 1;
				else return 0;
			}
		}
		return 0;
	}
		
	@Override
	public List<LeafValue> getNext() {
		// TODO Auto-generated method stub
		if (Iterator.hasNext())
			return Iterator.next();
		else 
			return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		getChild().close();
	}

	@Override
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
	
	/*
	public void orderby() {
		final int[] idx_order = new int[orderByElements.size()];
		final boolean[] isAsc = new boolean[orderByElements.size()];
		int idx = 0;
		for (int i = 0; i < orderByElements.size(); i++)
		{
			isAsc[i] = true;
			Column colName = (Column) ((OrderByElement) orderByElements.get(i)).getExpression();

			String tabName = colName.getTable().getName(); // OrderBy TableName
			String colmName = colName.getColumnName(); // OrderBy ColumnName
			for (int j = 0; j < this.tupleSchema.size(); j++)
			{
				Column cDef = this.tupleSchema.get(j);
				
				if(tabName==null || "".equals(tabName))
				{
					if (colmName.equals(cDef.getColumnName()))
					{
						idx_order[idx] = j;
						isAsc[idx ++] = ((OrderByElement) orderByElements.get(i)).isAsc();
					}	
				}
				else
				{						
					if (colmName.equals(cDef.getColumnName()) && tabName.equals(cDef.getTable().getName()))
					{
						idx_order[idx] = j;
						isAsc[idx ++] = ((OrderByElement) orderByElements.get(i)).isAsc();
					}					
				}
			}
		}
		
		comp = new Comparator<List<LeafValue>>() 
		{
			public int compare(List<LeafValue> list1, List<LeafValue> list2) 
			{
				int cnt_order ;
				int result = 0;
				String tmp1 = null;
				String tmp2 = null; 
				double tmp1_d = -1;
				double tmp2_d = -1;
				long tmp1_l = 0;
				long tmp2_l = 0;
				for(cnt_order = idx_order.length-1;cnt_order >=0;cnt_order--)
				{
					if (list1.get(idx_order[cnt_order]) instanceof DateValue || list1.get(idx_order[cnt_order]) instanceof StringValue) {
						tmp1 = list1.get(idx_order[cnt_order]).toString();
						tmp2 = list2.get(idx_order[cnt_order]).toString();
					}
					else if (list1.get(idx_order[cnt_order]) instanceof LongValue) {
						try {
							tmp1_d = list1.get(idx_order[cnt_order]).toDouble();
						} catch (InvalidLeaf e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						try {
							tmp2_d = list2.get(idx_order[cnt_order]).toDouble();
						} catch (InvalidLeaf e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else if (list1.get(idx_order[cnt_order]) instanceof LongValue) {
						try {
							tmp1_l = list1.get(idx_order[cnt_order]).toLong();
						} catch (InvalidLeaf e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						try {
							tmp2_l = list2.get(idx_order[cnt_order]).toLong();
						} catch (InvalidLeaf e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if (tmp1 != null) {
						if (tmp1.compareTo(tmp2) != 0)
						{
							if (isAsc[cnt_order])
								result = tmp1.compareTo(tmp2);
							else
								result = tmp2.compareTo(tmp1);
						}
					}
					else if (tmp1_d != -1 || tmp2_d != -1) {
						if (Double.compare(tmp1_d, tmp2_d) != 0)
						{
							if (isAsc[cnt_order])
								result = Double.compare(tmp1_d, tmp2_d);
							else
								result = Double.compare(tmp2_d, tmp1_d);
						}
					}
					else {
						if (Long.valueOf(tmp1_l).compareTo(Long.valueOf(tmp2_l)) != 0)
						{
							if (isAsc[cnt_order])
								result = Long.valueOf(tmp1_l).compareTo(Long.valueOf(tmp2_l));
							else
								result = Long.valueOf(tmp2_l).compareTo(Long.valueOf(tmp1_l));
						}
					}
					
					
				}
				return result;
			}
		};
	}
	
	public void sort()
	{		
		List<LeafValue> row =  getChild().getNext();
		while(row != null)
		{
			resultTable.add(row);
			row =  getChild().getNext();
		}
		Collections.sort(resultTable, comp);
		
	}*/
}
