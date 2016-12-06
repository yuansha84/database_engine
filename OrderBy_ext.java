import java.util.*;
import java.lang.*;
import java.io.*;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.expression.*;

public class OrderBy_ext extends RAOperator{
	private List<OrderByElement> orderByElements;
	private List<Column> tupleSchema;
	Comparator<List<LeafValue>> comp = null;
	//Iterator<List<LeafValue>> Iterator;
	Runtime runtime;
	
	static final int Tuple_num=5000;
	static final int Bucket_size=100;
	
	private File swap;
	private List<File> subLists;
	private List<List<List<LeafValue>>> buckets;
	private List<BufferedReader> readers;
	private List<LeafValue> minTuple;
	private int minInd;
	private List<String> fieldTypes;

	public OrderBy_ext(List<OrderByElement> eles,RAOperator op,File swapDir){
		orderByElements=eles;
		super.setLeftChild(op);
		setTupleSchema();
		swap=swapDir;
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
		//runtime=Runtime.getRuntime();
		//System.out.println(runtime.freeMemory());
		comp=new Comparator<List<LeafValue>>(){
			public int compare(List<LeafValue> t1,List<LeafValue> t2){
				for(int i=0;i<orderByElements.size();i++){
					int ind=indexOf((Column)orderByElements.get(i).getExpression(),tupleSchema);
					boolean isAsc=orderByElements.get(i).isAsc();
					int cr=compareLeaf(t1.get(ind),t2.get(ind),isAsc);
					if(cr!=0)
						return cr;
				}
				return 0;
			}};
		fieldTypes=new ArrayList<String>();
		subLists=formSublists();
		buckets=new ArrayList<List<List<LeafValue>>>();
		readers=new ArrayList<BufferedReader>();
		try{
		for(int i=0;i<subLists.size();i++){
			buckets.add(new ArrayList<List<LeafValue>>());
			readers.add(new BufferedReader(new FileReader(subLists.get(i))));
			fillBucket(buckets.get(i),readers.get(i),Bucket_size);
		}}catch(IOException e){
			e.printStackTrace();
		}
		//System.out.println(runtime.freeMemory());
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
			if(asc)
				return (int)(((LongValue)v1).getValue()-((LongValue)v2).getValue());
			else
				return (int)(((LongValue)v2).getValue()-((LongValue)v1).getValue());
		}
		if(v1 instanceof DoubleValue){
			if(asc)
				return (int)(((DoubleValue)v1).getValue()-((DoubleValue)v2).getValue());
			else
				return (int)(((DoubleValue)v1).getValue()-((DoubleValue)v2).getValue());
		}
		return 0;
	}
	
	List<File> formSublists(){
		List<File> files=new ArrayList<File>();
		int count=0;
		int filenum=1;
		List<List<LeafValue>> resultTable=new ArrayList<List<LeafValue>>();
		List<LeafValue> tuple;
		RAOperator child=getChild();
		child.open();
		//int counter=0;
		while((tuple=child.getNext())!=null){
			//counter++;
			//set the fieldTypes to be used for typing records in the sublists 
			if(fieldTypes.size()==0){
				for(int i=0;i<tuple.size();i++){
					if(tuple.get(i) instanceof StringValue){
						fieldTypes.add("string");
					}else if(tuple.get(i) instanceof LongValue){
						fieldTypes.add("long");
					}else if(tuple.get(i) instanceof DoubleValue){
						fieldTypes.add("double");
					}else if(tuple.get(i) instanceof DateValue){
						fieldTypes.add("date");
					}else{
						
					}
				}
			}
			
			if(count<Tuple_num){
				resultTable.add(tuple);
				count++;
			}else{//sort the resultTable and make a new sublist file
				Collections.sort(resultTable,comp);
				String filename="sublist"+filenum+".csv";
				File newfile=makeFileFrom(resultTable,filename,swap);
				files.add(newfile);
				//System.out.println(runtime.totalMemory()-runtime.freeMemory());
				resultTable.clear();
				count=0;
				filenum++;
				System.gc();
				//System.out.println(runtime.totalMemory()-runtime.freeMemory());
			}
		}
		if(count>0){
			Collections.sort(resultTable,comp);
			String filename="sublist"+filenum+".csv";
			File newfile=makeFileFrom(resultTable,filename,swap);
			files.add(newfile);
			//System.out.println(runtime.totalMemory()-runtime.freeMemory());
			resultTable.clear();
			System.gc();
			//System.out.println(runtime.totalMemory()-runtime.freeMemory());
		}
		child.close();
		//System.out.println(counter);
		return files;
	}
	
	public File makeFileFrom(List<List<LeafValue>> table,String filename,File dir){
		File newFile=new File(dir,filename);
		try{
		BufferedWriter writer=new BufferedWriter(new FileWriter(newFile));
		List<LeafValue> ttuple;
		String line;
		newFile.createNewFile();
		for(int i=0;i<table.size();i++){
			line=HashJoin_ext.tupleToString(table.get(i));
			writer.write(line);
			writer.newLine();
		}
		writer.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return newFile;	
	}
	
	public void fillBucket(List<List<LeafValue>> bucket,BufferedReader reader,int bsize){
		List<LeafValue> tuple;
		int counter=0;
		try{
		while((tuple=HashJoin_ext.parseLine(reader.readLine(),fieldTypes))!=null&&counter<bsize){
				bucket.add(tuple);	
		}}catch(IOException e){
			e.printStackTrace();
		}
		
	}
	@Override
	public List<LeafValue> getNext() {
		// TODO Auto-generated method stub
		if(buckets.size()==0){//all the sublists have been output
			return null;
		}
		minTuple=buckets.get(0).get(0);//be the min element in the first bucket
		minInd=0;
		for(int i=1;i<buckets.size();i++){
			List<LeafValue> cur=buckets.get(i).get(0);
			if(comp.compare(cur,minTuple)<0){
				minTuple=cur;
				minInd=i;
			}
		}
		List<List<LeafValue>> minBuc=buckets.get(minInd);
		minBuc.remove(0);
		if(minBuc.size()==0){
			fillBucket(minBuc,readers.get(minInd),Bucket_size);
			if(minBuc.size()==0){
				buckets.remove(minInd);
				try{
				readers.get(minInd).close();
				readers.remove(minInd);
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		}
		return minTuple;
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		for(int i=0;i<subLists.size();i++){
			subLists.get(i).delete();
		}
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

}
