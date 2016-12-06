import java.util.*;
import java.io.*;
import java.sql.SQLException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class HashJoin_ext extends RAOperator{
	private Expression condition;
	private List<Column> tupleSchema;
	
	private List<Expression> lexps;
	private List<Expression> rexps;
	
	private List<LeafValue> newTuple;
	
	//private List<List<List<LeafValue>>> buckets;
	private List<File> lfiles;
	private List<File> rfiles;
	private List<String> ltupleTypes;
	private List<String> rtupleTypes;
	private int bucketInd;
	private File swapDir;
 
	//constants for building buckets for each relation
	static final int Bucket_num=1000;
	static final int Bucket_size=50;
	private String prefix; //prefix for bucket file names
	onePassJoin bucketJoin;
	
	public HashJoin_ext(){
		condition=null;
		tupleSchema=null;
	}
	
	public HashJoin_ext(Expression cond,RAOperator leftOp,RAOperator rightOp,File sdir){
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
				lexps.add(right);
				rexps.add(left);
			}
		}
		swapDir=sdir;
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
	
	public List<Column> getOldSchema(){//no alias will happen here,so just return null
		return null;
	}
	//implement the RAIterator interface
	
	private void writeBucket(List<List<LeafValue>> buffer,BufferedWriter writer){
		int size=buffer.size();
		for(int i=0;i<size;i++){
			List<LeafValue> tuple=buffer.get(i);
			String rec=tupleToString(tuple);
			try{
			writer.write(rec);
			writer.newLine();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		
			
	}
	
	public static String tupleToString(List<LeafValue> tuple){
		String rs="";
		for(int i=0;i<tuple.size();i++){
			LeafValue comp=tuple.get(i);
			String val;
			if(comp instanceof StringValue){
				val=((StringValue)comp).getValue();
			}else {
				val=comp.toString();
			}
			if(i==0)
				rs+=val;
			else
				rs+="|"+val;
		}
		return rs;
	}
	
	public List<File> createBucketFiles(RAOperator source,String sname,int nbucket,int bucket_size,File dir,List<String> fieldTypes,List<Expression> lexps){
		//Runtime runtime=Runtime.getRuntime();
		//System.out.println(runtime.freeMemory());
		if(source==null)
			return null;
		List<File> files=new ArrayList<File>();
		List<BufferedWriter> fileWriters=new ArrayList<BufferedWriter>();
		List<List<List<LeafValue>>> buckets=new ArrayList<List<List<LeafValue>>>();
		String filename;
		File file;
		List<LeafValue> tuple;
		for(int i=0;i<nbucket;i++){
			filename=sname+i+".csv";
			file=new File(dir,filename);
			
			try{
				file.createNewFile();
				files.add(file);
				BufferedWriter bufferWriter=new BufferedWriter(new FileWriter(file));
				fileWriters.add(bufferWriter);
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		source.open();
		for(int i=0;i<nbucket;i++){//allocate Bucket_num buckets
			buckets.add(new ArrayList<List<LeafValue>>());
		}
		//System.out.println(runtime.totalMemory()-runtime.freeMemory());
		while((tuple=source.getNext())!=null){
			//determine the field types from the first ltuple
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
			
			String key="";
			try{
			ExpEval eval=new ExpEval(source.getTupleSchema(),tuple);
			for(int i=0;i<lexps.size();i++){
				key+=(eval.eval(lexps.get(i))).toString();
			}}catch(SQLException e){
				e.printStackTrace();
			}
			int index=(key.hashCode()&0x7fffffff)%nbucket;
			buckets.get(index).add(tuple);
			if(buckets.get(index).size()>=bucket_size){//the bucket is full, write it out		
				BufferedWriter writer=fileWriters.get(index);
				writeBucket(buckets.get(index),writer);
				//empty the bucket and collects memory back
				buckets.get(index).clear();
				System.gc();
			}
		}
		//System.out.println(runtime.totalMemory()-runtime.freeMemory());
		source.close();
		for(int i=0;i<nbucket;i++){
			writeBucket(buckets.get(i),fileWriters.get(i));
			try{
			fileWriters.get(i).close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		/*buckets.clear();
		fileWriters.clear();
		System.gc();*/
		return files;
	}
	
	public static List<LeafValue> parseLine(String line,List<String> fieldTypes){
		if(line==null)
			return null;
		List<LeafValue> rs=new ArrayList<LeafValue>();
		String[] items=line.split("\\|");
		for(int i=0;i<items.length;i++){
			LeafValue value=parseItem(items[i],fieldTypes.get(i));
			rs.add(value);
		}
		return rs;
	}
	
	public static LeafValue parseItem(String item,String dataType){
		LeafValue leaf;
		if(dataType.equals("string")){
			return new StringValue('\''+item+'\'');
		}
		else if(dataType.equals("long"))
			return new LongValue(item);
		else if(dataType.equals("decimal"))
			return new DoubleValue(item);
		else if(dataType.equals("date"))
			return new DateValue(' '+item+' ');
		else return null;
	}
	
	
	class onePassJoin{
		private List<Expression> lexps;
		private List<Expression> rexps;
		private File lf;
		private File rf;
		private List<String> ltupleTypes;
		private List<String> rtupleTypes;
		
		private BufferedReader lfReader;
		private BufferedReader rfReader;
		private HashMap<String,List<ArrayList<LeafValue>>> hsm;
		private List<LeafValue> ltuple;
		private int rindex;
		private List<LeafValue> rtuple;
		private List<ArrayList<LeafValue>> slot;
		private List<LeafValue> newTuple;

		
		public onePassJoin(List<Expression> l_exps,List<Expression> r_exps,File llf,File rrf,List<String> lTypes,List<String> rTypes){
			lexps=l_exps;
			rexps=r_exps;
			lf=llf;
			rf=rrf;
			ltupleTypes=lTypes;
			rtupleTypes=rTypes;
		}
		
		//getters and setters omitted
		
		//implement the iterator interface
		public void open(){
			try{
			lfReader=new BufferedReader(new FileReader(lf));
			rfReader=new BufferedReader(new FileReader(rf));
			hsm=new HashMap<String,List<ArrayList<LeafValue>>>();
			while((ltuple=parseLine(lfReader.readLine(),ltupleTypes))!=null){
				String key="";
				try{
				ExpEval eval=new ExpEval(getLeftChild().getTupleSchema(),ltuple);
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
			while((rtuple=parseLine(rfReader.readLine(),rtupleTypes))!=null){
				String rkey="";
				try{
				ExpEval eval=new ExpEval(getRightChild().getTupleSchema(),rtuple);
				for(int i=0;i<rexps.size();i++){
					rkey+=(eval.eval(rexps.get(i))).toString();
				}}catch(SQLException e){
					e.printStackTrace();
				}
				if((slot=hsm.get(rkey))==null){
					rtuple=parseLine(rfReader.readLine(),rtupleTypes);
				}else{
					break;
				}
			}
			}catch(FileNotFoundException fe){
				fe.printStackTrace();
			}catch(IOException e){
				e.printStackTrace();
			}
			
		}
		
		public List<LeafValue> getNext(){
			if(rtuple==null)
				return null;
		
			if(rindex>=slot.size()){
				try{
				rindex=0;
				rtuple=parseLine(rfReader.readLine(),rtupleTypes);
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
						rtuple=parseLine(rfReader.readLine(),rtupleTypes);
					}else{
						break;
					}
				}}catch(IOException e){
					e.printStackTrace();
				}
				return getNext();
			}else{
				newTuple=new ArrayList<LeafValue>();
				newTuple.addAll(slot.get(rindex));
				rindex++;
				newTuple.addAll(rtuple);
				return newTuple;
			}
			
		}
		
		public void close(){
			try{
			lfReader.close();
			rfReader.close();
			}catch(IOException e){
				e.printStackTrace();
			}
			System.gc();
		}
	}
	
	public void open(){
		ltupleTypes=new ArrayList<String>();
		rtupleTypes=new ArrayList<String>();
		//RAOperator.printTupleSchema(getLeftChild().getTupleSchema());
		List<Column> cols=RATree.getColumnFromCond(condition);
		prefix="";
		for(int i=0;i<cols.size();i++)
			prefix+=cols.get(i).getColumnName();
		lfiles=createBucketFiles(getLeftChild(),prefix+"left",Bucket_num,Bucket_size,swapDir,ltupleTypes,lexps);
		System.gc();
		
		rfiles=createBucketFiles(getRightChild(),prefix+"right",Bucket_num,Bucket_size,swapDir,rtupleTypes,rexps);
		System.gc();
		//find the first nonempty pair of bucket files to join 
		for(bucketInd=0;bucketInd<Bucket_num;bucketInd++){
			File left=lfiles.get(bucketInd);
			File right=rfiles.get(bucketInd);
			if(left.length()!=0&&right.length()!=0)
				break;
		}
		if(bucketInd<Bucket_num){
			bucketJoin=new onePassJoin(lexps,rexps,lfiles.get(bucketInd),rfiles.get(bucketInd),ltupleTypes,rtupleTypes);
			bucketJoin.open();
			bucketInd++;
		}
		
	}
	
	public List<LeafValue> getNext(){
		if(bucketInd>=Bucket_num)
			return null;
		if((newTuple=bucketJoin.getNext())!=null)
			return newTuple;
		else{
			bucketJoin.close();
			bucketJoin=null;
			System.gc();
			while(bucketInd<Bucket_num){
				File left=lfiles.get(bucketInd);
				File right=rfiles.get(bucketInd);
				if(left.length()!=0&&right.length()!=0)
					break;
				bucketInd++;
			}
			if(bucketInd<Bucket_num){
				bucketJoin=new onePassJoin(lexps,rexps,lfiles.get(bucketInd),rfiles.get(bucketInd),ltupleTypes,rtupleTypes);
				bucketJoin.open();
				bucketInd++;
			}
			return getNext();
		}
		
	}
	
	public void close(){
		for(int i=0;i<lfiles.size();i++){
			lfiles.get(i).delete();
			rfiles.get(i).delete();
		}
	}

}

