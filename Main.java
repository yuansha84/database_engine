
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
//import java.lang.*;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

import com.sleepycat.je.*;

public class Main {
	static File dir;//dir is the directory for data files
	static File swap;//swap is the swapping directory for storing temperary files
	static File db;//db is the db directory for storing the Database objects
	static boolean load;
	static ArrayList<File> sqlFiles;//query files
	static HashMap<String,CreateTable> tables;//tables
	public static void main(String[] args){
		dir=null;
		swap=null;//swap is the swapping directory for storing temperary files
		db=null;//db is the db directory for storing the Database objects
		load=false;
		sqlFiles=new ArrayList<File>();//query files
		tables=new HashMap<String,CreateTable>();
		//get from arguments and put the files
		for(int i=0;i<args.length;i++){
			if(args[i].equals("--data")){
				dir=new File(args[i+1]);
				i++;
			}else if(args[i].equals("--swap")){
				swap=new File(args[i+1]);
				i++;
			}else if(args[i].equals("--db")){
				db=new File(args[i+1]);
				i++;
			}else if(args[i].equals("--load")){
				load=true;
			}else{
				sqlFiles.add(new File(args[i]));
			}
		}
		if(load==true){
			for(File sql:sqlFiles){//all the statements in sql are CREATE TABLE statement
				try{
					FileReader reader=new FileReader(sql);
					CCJSqlParser sqlParser=new CCJSqlParser(reader);
					Statement stmt;
					while((stmt=sqlParser.Statement())!=null){
						CreateTable ct=(CreateTable)stmt;
						String tableName=ct.getTable().getName();
						tables.put(tableName, ct);
					}
					
				}catch(IOException ioe){
					ioe.printStackTrace();
				}catch(ParseException pe){
					pe.printStackTrace();
				}
			}
			//long start,end;
			//start=System.currentTimeMillis();
			createDbs();
			//end=System.currentTimeMillis();
			//System.out.println("elapsed time:"+(end-start)+" milliseconds");
			/*
			Environment env=null;
			Database lineitem=null;
			Cursor cursor=null;
			DiskOrderedCursor dcursor=null;
			DiskOrderedCursorConfig dcurc=new DiskOrderedCursorConfig();
			SecondaryDatabase shipdate=null;
			SecondaryCursor secCursor=null;
			try{
			start=System.currentTimeMillis();
			EnvironmentConfig envConfig=new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setLocking(false);

			env=new Environment(db,envConfig);
			DatabaseConfig dbConfig=new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			lineitem=env.openDatabase(null, "ORDERS", dbConfig);
			SecondaryConfig secConfig=new SecondaryConfig();
			secConfig.setSortedDuplicates(true);
			secConfig.setKeyCreator(new SimpleSecKeyCreator("orderdate",tables.get("ORDERS").getColumnDefinitions()));
			shipdate=env.openSecondaryDatabase(null, "ORDERS.orderdate", lineitem, secConfig);
			secCursor=shipdate.openCursor(null, null);		
			//dcursor=lineitem.openCursor(dcurc);
			//cursor=lineitem.openCursor(null,null);
			DatabaseEntry fkey=new DatabaseEntry();
			DatabaseEntry fdata=new DatabaseEntry();
			//while(dcursor.getNext(fkey, fdata, LockMode.READ_UNCOMMITTED)==OperationStatus.SUCCESS){
			//}
			List<ColumnDefinition> colDefs=tables.get("ORDERS").getColumnDefinitions();
			List<String> dataTypes=new ArrayList<String>();
			int ind=-1;
			for(int i=0;i<colDefs.size();i++){
				ColumnDefinition coldef=colDefs.get(i);
				if(coldef.getColumnName().toLowerCase().equals("orderdate"))
					ind=i;
				dataTypes.add(coldef.getColDataType().getDataType().toLowerCase());
				System.out.print(coldef.getColDataType().getDataType());
				}
			end=System.currentTimeMillis();
			System.out.println("elapsed time:"+(end-start)+" milliseconds");
			start=System.currentTimeMillis();
			String odate="1993-10-01";
			DatabaseEntry searchKey=new DatabaseEntry(odate.getBytes());
			if(secCursor.getSearchKey(searchKey, fdata, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				while(secCursor.getNext(fkey, fdata, LockMode.DEFAULT)==OperationStatus.SUCCESS){
					String[] rec=entryToStrings(fdata,dataTypes);
					//System.out.println(Arrays.toString(rec));
					if(rec[ind].compareTo("1994-01-01")>=0)
						break;
				}
			}
			end=System.currentTimeMillis();
			System.out.println("elapsed time:"+(end-start)+" milliseconds");
			}catch(DatabaseException dbe){
				System.out.println("Error accessing database."+dbe);
			}finally{
				if(secCursor!=null)
					secCursor.close();
				if(shipdate!=null)
					shipdate.close();
				if(lineitem!=null)
					lineitem.close();
				if(env!=null)
					env.close();
			}
			tableScan("ORDERS");*/
		}else{
		//evaluate the queries in query files with the data files
		for(File sql : sqlFiles){
			//long start,end;
			//start=System.currentTimeMillis();
			try{
			FileReader reader=new FileReader(sql);
			CCJSqlParser sqlParser=new CCJSqlParser(reader);
			Statement stmt;
			while((stmt=sqlParser.Statement())!=null){
				if(stmt instanceof CreateTable){
					CreateTable ct=(CreateTable)stmt;
					String tableName=ct.getTable().getName();
					tables.put(tableName, ct);
				}else if(stmt instanceof Select){
					Select st=(Select)stmt;
					SelectBody sby=st.getSelectBody();
					//begins to evaluate
					RATree tree=new RATree(sby,tables,dir,swap,db);
					//RATree.printTree(tree.ra_tree);
					tree.evaluate();
				}else{
					System.out.println("I don't know what to do");
				}
			}
			//end=System.currentTimeMillis();
			//System.out.println("elapsed time:"+(end-start)+" milliseconds");
			}catch(IOException e){
				e.printStackTrace();
			}catch(ParseException e){
				e.printStackTrace();
			}
			
		}	
		
	}}
	
	//funct: public static void createDbs();
	//this function is used to create Database objects for tables
	public static void createDbs(){
		Environment myDbEnv=null;
		List<Database> myDblst=new ArrayList<Database>();
		Database myDb=null;
		Set<String> tbNames=tables.keySet();
		
		//create the primary databases
		try{
			//open the environment
			EnvironmentConfig envConfig=new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setLocking(false);
			//envConfig.setCachePercent(90);
			//envConfig.setCacheSize(1000000000);
			myDbEnv=new Environment(db,envConfig);
			//System.out.println("The cach percent is "+envConfig.getCachePercent()+" ");
			//System.out.println("The default locking mode  is "+envConfig.getLocking()+" ");
			//open and load databases
			Iterator<String> iter=tbNames.iterator();
			DatabaseConfig dbConfig=new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			dbConfig.setDeferredWrite(true);
			while(iter.hasNext()){
				String tbn=(String)iter.next();
				myDb=myDbEnv.openDatabase(null, tbn, dbConfig);
				myDblst.add(myDb);
				CreateTable crt=tables.get(tbn);
				List<ColumnDefinition> colDefs=crt.getColumnDefinitions();
				List<String> dataTypes=new ArrayList<String>();
				List<Integer> priInd=new ArrayList<Integer>();
				List<String> priTypes=new ArrayList<String>();
				List<String> priItems=new ArrayList<String>();
				ColumnDefinition coldef;
				List<String> colspec;
				BufferedReader bufreader;
				DatabaseEntry myKey;
				DatabaseEntry myData;
				//get data types and the primary keys' indices
				for(int i=0;i<colDefs.size();i++){
					coldef=colDefs.get(i);
					dataTypes.add(coldef.getColDataType().getDataType().toLowerCase());
					colspec=coldef.getColumnSpecStrings();
					if(colspec!=null&&colspec.get(0).toUpperCase().equals("PRIMARY")){
						priInd.add(i);
						priTypes.add(coldef.getColDataType().getDataType().toLowerCase());
					}
				}
				//read from data file and write to database
				try{
					bufreader=new BufferedReader(new FileReader(new File(dir,tbn.toUpperCase()+".dat")));
					String line;
					String[] items;
					int counter=0;
					while((line=bufreader.readLine())!=null){
						items=line.split("\\|");
						//for(int j=0;j<1000;j++);
						/*
						List<String> litem=new ArrayList<String>();
						for(int i=0;i<items.length;i++)
							litem.add(items[i]);
						myKey=new DatabaseEntry(("myKey"+counter).getBytes());
						//myData=createEntry(items,dataTypes);
						myData=new DatabaseEntry(line.getBytes());
						myDb.put(null, myKey, myData);
						counter++;*/
						
						for(int j=0;j<priInd.size();j++)
							priItems.add(items[priInd.get(j)]);
						myKey=createEntry(priItems.toArray(new String[0]),priTypes);
						myData=createEntry(items,dataTypes);
						myDb.put(null, myKey, myData);
						priItems.clear();
					}
					//System.out.println("The cach size is "+envConfig.getCacheSize()+" bytes");
				}catch(IOException e){
					e.printStackTrace();
				}
				
			}
		}catch(DatabaseException dbe){
			//
		}finally{
			for(int i=0;i<myDblst.size();i++)
				myDblst.get(i).close();
			myDbEnv.close();
		}
		
		//create secondary databases
		List<Database> dbl=new ArrayList<Database>();
		List<SecondaryDatabase> secdbl=new ArrayList<SecondaryDatabase>();
		try{
			EnvironmentConfig envConfig=new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setLocking(false);
			myDbEnv=new Environment(db,envConfig);
			Iterator<String> iter=tbNames.iterator();
			SecondaryConfig secDbConfig=new SecondaryConfig();
			secDbConfig.setAllowCreate(true);
			secDbConfig.setSortedDuplicates(true);
			secDbConfig.setAllowPopulate(true);
			secDbConfig.setDeferredWrite(true);
			DatabaseConfig dbConfig=new DatabaseConfig();
			SecondaryDatabase secDb=null;
			while(iter.hasNext()){
				String tbn=(String)iter.next();
				CreateTable crt=tables.get(tbn);
				List<ColumnDefinition> colDefs=crt.getColumnDefinitions();
				if(tbn.toUpperCase().equals("LINEITEM")){//for the primary database LINEITEM
					myDb=myDbEnv.openDatabase(null, tbn, dbConfig);
					SimpleSecKeyCreator shipdateKey=new SimpleSecKeyCreator("shipdate",colDefs);
					secDbConfig.setKeyCreator(shipdateKey);
					secDb=myDbEnv.openSecondaryDatabase(null,"LINEITEM.shipdate", myDb, secDbConfig);
					secdbl.add(secDb);
					//dbl.add(myDb);
					//create other secondary databases for LINEITEM
					
					SimpleSecKeyCreator discountKey=new SimpleSecKeyCreator("discount",colDefs);
					secDbConfig.setKeyCreator(discountKey);
					secDb=myDbEnv.openSecondaryDatabase(null,"LINEITEM.discount", myDb, secDbConfig);
					secdbl.add(secDb);
					SimpleSecKeyCreator quantityKey=new SimpleSecKeyCreator("quantity",colDefs);
					secDbConfig.setKeyCreator(quantityKey);
					secDb=myDbEnv.openSecondaryDatabase(null,"LINEITEM.quantity", myDb, secDbConfig);
					secdbl.add(secDb);
					SimpleSecKeyCreator returnflagKey=new SimpleSecKeyCreator("returnflag",colDefs);
					secDbConfig.setKeyCreator(returnflagKey);
					secDb=myDbEnv.openSecondaryDatabase(null,"LINEITEM.returnflag", myDb, secDbConfig);
					secdbl.add(secDb);
					SimpleSecKeyCreator shipmodeKey=new SimpleSecKeyCreator("shipmode",colDefs);
					secDbConfig.setKeyCreator(shipmodeKey);
					secDb=myDbEnv.openSecondaryDatabase(null,"LINEITEM.shipmode", myDb, secDbConfig);
					secdbl.add(secDb);
					dbl.add(myDb);
				}else if(tbn.toUpperCase().equals("CUSTOMER")){
					myDb=myDbEnv.openDatabase(null, tbn, dbConfig);
					SimpleSecKeyCreator mktsegmentKey=new SimpleSecKeyCreator("mktsegment",colDefs);
					secDbConfig.setKeyCreator(mktsegmentKey);
					secDb=myDbEnv.openSecondaryDatabase(null,"CUSTOMER.mktsegment", myDb, secDbConfig);
					secdbl.add(secDb);
					dbl.add(myDb);
				}else if(tbn.toUpperCase().equals("ORDERS")){
					myDb=myDbEnv.openDatabase(null, tbn, dbConfig);
					SimpleSecKeyCreator orderdateKey=new SimpleSecKeyCreator("orderdate",colDefs);
					secDbConfig.setKeyCreator(orderdateKey);
	
					secDb=myDbEnv.openSecondaryDatabase(null,"ORDERS.orderdate", myDb, secDbConfig);
					secdbl.add(secDb);
					dbl.add(myDb);
				}else if(tbn.toUpperCase().equals("REGION")){
					myDb=myDbEnv.openDatabase(null, tbn, dbConfig);
					SimpleSecKeyCreator nameKey=new SimpleSecKeyCreator("name",colDefs);
					secDbConfig.setKeyCreator(nameKey);
					secDb=myDbEnv.openSecondaryDatabase(null,"REGION.name", myDb, secDbConfig);
					secdbl.add(secDb);
					dbl.add(myDb);
				}
				
			}
			/*
			Long search=2L;
			ByteBuffer buf=ByteBuffer.allocate(8);
			buf.putLong(search);
			DatabaseEntry searchKey=new DatabaseEntry(buf.array());
			DatabaseEntry odata=new DatabaseEntry();
			if(myDb.get(null, searchKey, odata, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				byte[] dddata=odata.getData();
				ByteArrayInputStream in=new ByteArrayInputStream(dddata);
				DataInputStream from=new DataInputStream(in);
			
				System.out.print(from.readLong());
				System.out.print('|');
				System.out.print(from.readLong());
				System.out.print('|');
				System.out.print(from.readUTF()+'|');
				System.out.print(from.readDouble());
				System.out.print('|');
				System.out.print(from.readUTF()+'|');
				System.out.print(from.readUTF()+'|');
				System.out.print(from.readUTF()+'|');
				System.out.print(from.readLong());
				System.out.print('|');
				System.out.println(from.readUTF());
				
			}
			
			String odate="1996-01-02";
			DatabaseEntry searchKey=new DatabaseEntry(odate.getBytes("UTF-8"));
			DatabaseEntry okey=new DatabaseEntry();
			DatabaseEntry odata=new DatabaseEntry();
			SecondaryCursor secCur=secDb.openCursor(null, null);
			OperationStatus ret=secCur.getSearchKey(searchKey, odata, LockMode.DEFAULT);
			while(ret==OperationStatus.SUCCESS){
				byte[] dddata=odata.getData();
				ByteArrayInputStream in=new ByteArrayInputStream(dddata);
				DataInputStream from=new DataInputStream(in);
			
				System.out.print(from.readLong());
				System.out.print('|');
				System.out.print(from.readLong());
				System.out.print('|');
				System.out.print(from.readUTF()+'|');
				System.out.print(from.readDouble());
				System.out.print('|');
				System.out.print(from.readUTF()+'|');
				System.out.print(from.readUTF()+'|');
				System.out.print(from.readUTF()+'|');
				System.out.print(from.readLong());
				System.out.print('|');
				System.out.println(from.readUTF());
				ret=secCur.getNextDup(searchKey, odata, LockMode.DEFAULT);
			}
			secCur.close();*/
		}catch(DatabaseException dbe){
			
		}finally{
			
			for(int i=0;i<secdbl.size();i++)
				secdbl.get(i).close();
			for(int i=0;i<dbl.size();i++)
				dbl.get(i).close();
			myDbEnv.close();
			
		}
		
	}
	
	public static  DatabaseEntry createEntry(String[] items,List<String> dataTypes){
		DatabaseEntry entry=new DatabaseEntry();
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		DataOutputStream to=new DataOutputStream(out);
		try{
		for(int i=0;i<dataTypes.size();i++){
			String dataType=dataTypes.get(i);
			if(dataType.equals("char")||dataType.equals("varchar")||dataType.equals("date")){
				to.writeUTF(items[i]);
			}
			else if(dataType.equals("int"))
				to.writeLong(Long.parseLong(items[i]));
			else if(dataType.equals("decimal"))
				to.writeDouble(Double.parseDouble(items[i]));
			
	}
		}catch(IOException ioe){
		 ioe.printStackTrace();
		}
		entry.setData(out.toByteArray());
		//System.out.println(et-st);
		return entry;
	}
	
	public static String[] entryToStrings(DatabaseEntry entry,List<String> dataTypes){
		ByteArrayInputStream in=new ByteArrayInputStream(entry.getData());
		DataInputStream from=new DataInputStream(in);
		String[] res=new String[dataTypes.size()];
		try{
		for(int i=0;i<dataTypes.size();i++){
			String dataType=dataTypes.get(i);
			if(dataType.equals("char")||dataType.equals("varchar")||dataType.equals("date")){
				res[i]=from.readUTF();
			}
			else if(dataType.equals("int"))
				res[i]=Long.toString(from.readLong());
			else if(dataType.equals("decimal"))
				res[i]=Double.toString(from.readDouble());
		}}catch(IOException e){
			
		}
		return res;
	}
	
	public static void tableScan(String tbl){
		Long st,end;
		st=System.currentTimeMillis();
		try{
		File t=new File(dir,tbl+".dat");
		BufferedReader bufferReader=new BufferedReader(new FileReader(t));
		String line=null;
		String[] items=null;
		while((line=bufferReader.readLine())!=null){
			items=line.split("\\|");
		}
		}catch(IOException e){
			e.printStackTrace();
		}
		end=System.currentTimeMillis();
		System.out.println("table scan for "+tbl+" costs "+(end-st)+" milliseconds");
	}
}
