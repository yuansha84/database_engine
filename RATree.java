import java.util.*;
import java.io.*;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.schema.*;

public class RATree {

	private SelectBody statement;//the sql query statement
	private HashMap<String,CreateTable> tableMap;
	public RAOperator ra_tree;
	private File dir;
	static private File swap;
	private File dbdir;
	
	public RATree(SelectBody stamt,HashMap<String,CreateTable> map,File dataDir,File swapDir,File dbdirec){
		statement=stamt;
		tableMap=map;
		dir=dataDir;
		swap=swapDir;
		dbdir=dbdirec;
		ra_tree=treeCons(statement);
		ra_tree=optimize(ra_tree);
		if(swap!=null)
			ra_tree=operatorReplace(ra_tree,swap);
	}
	
	public static RAOperator operatorReplace(RAOperator ra_tree,File swap){
		if(ra_tree==null)
			return null;
		RAOperator left=ra_tree.getLeftChild();
		RAOperator right=ra_tree.getRightChild();
		if(ra_tree instanceof OrderBy){
			ra_tree=new OrderBy_ext(((OrderBy)ra_tree).getOrderByElements(),operatorReplace(left,swap),swap);
		}else if(ra_tree instanceof EquiJoin){
			ra_tree=new HashJoin_ext(((EquiJoin)ra_tree).getCondition(),operatorReplace(left,swap),operatorReplace(right,swap),swap);
		}else if(ra_tree instanceof CatesianProduct){
			ra_tree=new Product_NLJ(operatorReplace(left,swap),operatorReplace(right,swap));
		}else if(ra_tree instanceof InputTable){
			//do nothing
		}else{
			ra_tree.setLeftChild(operatorReplace(left,swap));
			ra_tree.setRightChild(operatorReplace(right,swap));
		}
		return ra_tree;
		
	}
	
	public static RAOperator optimize(RAOperator ra_tree){
		if(ra_tree==null){
			return null;
		}
		if(ra_tree instanceof Selection){
			RAOperator child=((Selection)ra_tree).getChild();
			//((Selection)ra_tree).printCondition();
			if( child instanceof CatesianProduct){//selection on top of product, optimize it
				Expression cond=((Selection)ra_tree).getCondition();
				List<Expression> conditions=getSeparatedCond(cond);
				RAOperator leftOp=child.getLeftChild();
				RAOperator rightOp=child.getRightChild();
				List<Expression> associatedWithLeft=new ArrayList<Expression>();//conditional only associated with left
				List<Expression> associatedWithRight=new ArrayList<Expression>();//conditional only associated with right
				List<Expression> associatedWithBoth=new ArrayList<Expression>();//conditional associated with left and right but is not an EqualsTo 
				List<Expression> associatedWithProduct=new ArrayList<Expression>();//conditional associated with left and right and is an EqualsTo 
				
				for(int i=0;i<conditions.size();i++){
					Expression con=conditions.get(i);
					List<Column> cols=getColumnFromCond(con);
					if(associatedWith(cols,leftOp.getTupleSchema())){
						if(associatedWith(cols,rightOp.getTupleSchema())){
							if(con instanceof EqualsTo){
								associatedWithProduct.add(con);
							}else{
								associatedWithBoth.add(con);
							}
						}else{//only associated with leftOp
							associatedWithLeft.add(con);
						}
					}else if(associatedWith(cols,rightOp.getTupleSchema())){
						associatedWithRight.add(con);
					}else{//error ,leave it empty now
						
					}
				}
				//System.out.println("left:"+associatedWithLeft.size()+" ,right:"+associatedWithRight.size()+" ,both:"+associatedWithBoth.size());
				if(associatedWithBoth.size()>0){
					Expression newCond=makeAndExp(associatedWithBoth);
					((Selection)ra_tree).setCondition(newCond);
				}else {
					ra_tree=child;
				}
				
				if(associatedWithProduct.size()>0){
					Expression joinCond=makeAndExp(associatedWithProduct);
					RAOperator join=new EquiJoin(joinCond,leftOp,rightOp);
					//join=new HashJoin_ext(joinCond,leftOp,rightOp,swap);
					if(ra_tree==child){
						ra_tree=join;
					}else{
					ra_tree.setLeftChild(join);
					}
				}
				
				if(associatedWithLeft.size()>0){
					Expression leftCond=makeAndExp(associatedWithLeft);
					RAOperator sel=new Selection(leftCond,leftOp);
					//System.out.print("left selection condition:");
					//((Selection)sel).printCondition();
					if(ra_tree instanceof Selection){
						((Selection)ra_tree).getChild().setLeftChild(optimize(sel));
					}else{
						ra_tree.setLeftChild(optimize(sel));
					}
				}
				
				if(associatedWithRight.size()>0){
					Expression rightCond=makeAndExp(associatedWithRight);
					RAOperator sel=new Selection(rightCond,rightOp);
					if(ra_tree instanceof Selection){
						((Selection)ra_tree).getChild().setRightChild(optimize(sel));
					}else{
						ra_tree.setRightChild(optimize(sel));
					}
				}
			}else if(child instanceof OrderBy||child instanceof Distinction||child instanceof Projection){
				if(child.getAlias()!=null){
					((Selection)ra_tree).setCondition(getOldCondition(((Selection)ra_tree).getCondition(),child.getOldSchema()));
				}
				//((Selection)ra_tree).printCondition();
				if(child instanceof Projection){
					List<SelectItem> selectItems=((Projection)child).getSelectItems();
					HashMap<String,Column> aliasMap=buildAliasMap(selectItems);
					((Selection)ra_tree).setCondition(getPrjExpression(((Selection)ra_tree).getCondition(),aliasMap));
				}
				//((Selection)ra_tree).printCondition();
				ra_tree.setLeftChild(child.getLeftChild());
				child.setLeftChild(optimize(ra_tree));
				//RAOperator.printTupleSchema(child.getTupleSchema());
				ra_tree=child;
			}else if(child instanceof Selection){
				Expression newCond=new AndExpression(((Selection)ra_tree).getCondition(),((Selection)child).getCondition());
				((Selection) child).setCondition(newCond);
				//((Selection) child).printCondition();
				ra_tree=optimize(child);
			}else if(child instanceof Aggregation){
				List<Expression> pushThrough=new ArrayList<Expression>();
				List<Expression> stayHere=new ArrayList<Expression>();
				List<Column> groupAttrs=((Aggregation)child).getGroupColumns();
				List<Column> functions=new ArrayList<Column>();
				functions.addAll(child.getTupleSchema());
				functions.removeAll(groupAttrs);
				List<Expression> conds=getSeparatedCond(((Selection)ra_tree).getCondition());
				List<Expression> groupAttrCond=new ArrayList<Expression>();
				List<Expression> functionsCond=new ArrayList<Expression>();
				for(int i=0;i<conds.size();i++){
					Expression con=conds.get(i);
					List<Column> cols=getColumnFromCond(con);
					if(associatedWith(cols,groupAttrs)&&!associatedWith(cols,functions)){//con involving only grouping attributes
						groupAttrCond.add(con);
					}else{
						functionsCond.add(con);
					}
				}
				if(groupAttrCond.size()!=0){//push the groupAttrCond down
					Expression newCond=makeAndExp(groupAttrCond);
					Expression oldCond=makeAndExp(functionsCond);
					Selection newSelect=new Selection(newCond,child.getLeftChild());
					child.setLeftChild(newSelect);
					((Selection)ra_tree).setCondition(oldCond);
					
				}
				ra_tree.setLeftChild(optimize(child));
			}else{//do nothing
				
			}
		}else {
			ra_tree.setLeftChild(optimize(ra_tree.getLeftChild()));
			ra_tree.setRightChild(optimize(ra_tree.getRightChild()));
		}
		return ra_tree;
	}
	
	public static Expression getOldCondition(Expression cond,List<Column> oldSchema){
		if(cond instanceof BinaryExpression){
			((BinaryExpression)cond).setLeftExpression(getOldCondition(((BinaryExpression)cond).getLeftExpression(),oldSchema));
			((BinaryExpression)cond).setRightExpression(getOldCondition(((BinaryExpression)cond).getRightExpression(),oldSchema));
			return cond;
		}else if(cond instanceof Between){
			((Between)cond).setLeftExpression(getOldCondition(((Between)cond).getLeftExpression(),oldSchema));
			return cond;
		}else if(cond instanceof InExpression){
			((InExpression)cond).setLeftExpression(getOldCondition(((InExpression)cond).getLeftExpression(),oldSchema));
			return cond;
		}else if(cond instanceof Column){
			return getOldColumn((Column)cond,oldSchema);
		}else{
			//System.out.println(cond.toString());
			return cond;
		}
	}
	
	public static Column getOldColumn(Column col,List<Column> oldSchema){
		//RAOperator.printColumn(col);
		//RAOperator.printTupleSchema(oldSchema);
		for(int i=0;i<oldSchema.size();i++){
			if(col.getColumnName().equals(oldSchema.get(i).getColumnName())){
				return oldSchema.get(i);
			}
		}
		System.out.println("getOldColumn(): Cannot find col");
		return null;
	}
	
	public static Expression getPrjExpression(Expression cond,HashMap<String,Column> aliasMap){
		if(cond instanceof BinaryExpression){
			((BinaryExpression)cond).setLeftExpression(getPrjExpression(((BinaryExpression)cond).getLeftExpression(),aliasMap));
			((BinaryExpression)cond).setRightExpression(getPrjExpression(((BinaryExpression)cond).getRightExpression(),aliasMap));
			return cond;
		}else if(cond instanceof Between){
			((Between)cond).setLeftExpression(getPrjExpression(((Between)cond).getLeftExpression(),aliasMap));
			return cond;
		}else if(cond instanceof InExpression){
			((InExpression)cond).setLeftExpression(getPrjExpression(((InExpression)cond).getLeftExpression(),aliasMap));
			return cond;
		}else if(cond instanceof Column){
			Column aliasName=aliasMap.get(((Column)cond).getColumnName());
			//RAOperator.printColumn(aliasName);
			if(aliasName==null)
				return cond;
			else {
				return aliasName;
			}
		}else{
			//System.out.println(cond.toString());
			return cond;
		}
		
	}
	public static HashMap<String,Column> buildAliasMap(List<SelectItem> selectItems){
		HashMap<String,Column> aliasMap=new HashMap<String,Column>();
		for(int i=0;i<selectItems.size();i++){
		SelectItem item=selectItems.get(i);
		if(item instanceof SelectExpressionItem){
			Expression exp=((SelectExpressionItem)item).getExpression();
			String alias=((SelectExpressionItem)item).getAlias();
			if(exp instanceof Column){
				Column newCol=new Column();
				newCol.setTable(((Column)exp).getTable());
				newCol.setColumnName(((Column)exp).getColumnName());
				if(alias!=null)
					aliasMap.put(alias,newCol);
			}else if(exp instanceof Function){
				Table empty=new Table();
				String func=((Function)exp).getName().toUpperCase();
				String colName;
				if(func.equals("COUNT"))
				colName=((Function)exp).getName();
				else colName=((Function)exp).getName()+((Function)exp).getParameters().toString();
				Column col=new Column(empty,colName);
				if(alias!=null)
					aliasMap.put(alias,col);
			}else{
				Table empty=new Table();
				Column col=new Column(empty,exp.toString());
				if(alias!=null)
					aliasMap.put(alias,col);
			}
		}
		}
		return aliasMap;
	}
	
	public static Expression makeAndExp(List<Expression> exps){
		if(exps==null||exps.size()==0)
			return null;
		else if(exps.size()==1)
		return exps.get(0);
		else{
		Expression newAnd=new AndExpression(exps.get(0),makeAndExp(exps.subList(1,exps.size())));
		return newAnd;
		}
	}
	
	public static List<Expression> getSeparatedCond(Expression cond){
		List<Expression> rs=new ArrayList<Expression>();
		if(cond instanceof AndExpression){
			Expression left=((AndExpression)cond).getLeftExpression();
			Expression right=((AndExpression)cond).getRightExpression();
			rs.addAll(getSeparatedCond(left));
			rs.addAll(getSeparatedCond(right));
		}else{
			rs.add(cond);
		}
		return rs;
	}
	
	public static List<Column> getColumnFromCond(Expression cond){
		List<Column> rs=new ArrayList<Column>();
		if(cond instanceof BinaryExpression){
			Expression left=((BinaryExpression)cond).getLeftExpression();
			Expression right=((BinaryExpression)cond).getRightExpression();
			rs.addAll(getColumnFromCond(left));
			rs.addAll(getColumnFromCond(right));
		}else if(cond instanceof Between){
			Expression left=((Between)cond).getLeftExpression();
			Expression start=((Between)cond).getBetweenExpressionStart();
			Expression end=((Between)cond).getBetweenExpressionEnd();
			rs.addAll(getColumnFromCond(left));
			rs.addAll(getColumnFromCond(start));
			rs.addAll(getColumnFromCond(end));
		}else if(cond instanceof InExpression){
			Expression left=((InExpression)cond).getLeftExpression();
			rs.addAll(getColumnFromCond(left));
		}if(cond instanceof Column){
			rs.add((Column)cond);
		}else{
			
		}
		return rs;
	}
	
	public static boolean associatedWith(List<Column> cols,List<Column> schema){
		boolean res=false;
		for(int i=0;i<cols.size();i++){
			if(RAOperator.indexOf(cols.get(i),schema)>=0){
				res=true;
				break;
			}
		}
		return res;
	}
	
	public void evaluate(){
		List<LeafValue> tuple;
		ra_tree.open();
		
		while((tuple=ra_tree.getNext())!=null){
			for(int i=0;i<tuple.size();i++){
				printLeafValue(tuple.get(i));
				if(i<tuple.size()-1)
					System.out.print('|');
				else
					System.out.print('\n');
			}
			
		}
		ra_tree.close();
	}
	
	public void printLeafValue(LeafValue val){
		if(val instanceof StringValue)
			System.out.print(((StringValue)val).getValue());
		else if(val instanceof LongValue)
			System.out.print(((LongValue)val).getValue());
		else if(val instanceof DoubleValue)
		System.out.print(((DoubleValue)val).getValue());
		else if(val instanceof DateValue)
			System.out.print(((DateValue)val).getValue());
		else{
			
		}
	}
	public RAOperator treeCons(SelectBody stmt){
		RAOperator curRoot=null;
		if(stmt instanceof PlainSelect){
			PlainSelect mySelect=(PlainSelect)stmt;
			FromItem item=mySelect.getFromItem();
			List<Join> joinItems=mySelect.getJoins();
			if(joinItems==null||joinItems.size()==0){//only one FromItem in the from clause,no Join.
				if(item instanceof Table){
					curRoot=new InputTable((Table)item,tableMap,dir,dbdir);
				}else if(item instanceof SubSelect){
					SelectBody stat=((SubSelect) item).getSelectBody();
					String alias=((SubSelect)item).getAlias();
					curRoot=treeCons(stat);
					curRoot.setAlias(alias);
				}else{
					//be empty now
				}
			}else{
			RAOperator productTree=productTreeCons(item,joinItems);
			curRoot=productTree;//curRoot be the productTree
			}
			//treat the where clause
			Expression cond1=mySelect.getWhere();
			//if the where clause is there, add a Selection
			if(cond1!=null){
			curRoot=new Selection(cond1,curRoot);//curRoot be the selection in the where clause
			}
			//treat the groupBy clause
			List<Column> groupAttrs=(List<Column>)mySelect.getGroupByColumnReferences();
			//treat the having clause
			Expression havingCond=mySelect.getHaving();
			//
			List<SelectItem> selectItems=mySelect.getSelectItems();
			List<Function> functions=findAggreFunctions(havingCond,selectItems);
			if(!functions.isEmpty())
			curRoot=new Aggregation(groupAttrs,functions,curRoot);
			if(havingCond!=null)
			curRoot=new Selection(havingCond,curRoot);
			
			Distinct dist=mySelect.getDistinct();
			curRoot=new Projection(selectItems,curRoot);
			if(dist!=null){
				curRoot=new Distinction(curRoot);
			}
			
			List<OrderByElement> orderElements=mySelect.getOrderByElements();
			if(orderElements!=null){
			curRoot=new OrderBy(orderElements,curRoot);
			//curRoot=new OrderBy_ext(orderElements,curRoot,swap);
			}
			Limit limit=mySelect.getLimit();
			if(limit!=null){
			curRoot=new RowLimit(limit.getRowCount(),curRoot);
			}
		}else if(stmt instanceof Union){
			List<PlainSelect> plainSelects=((Union) stmt).getPlainSelects();
			curRoot=treeCons(plainSelects.get(0));
			for(int i=1;i<plainSelects.size();i++){
				RAOperator right=treeCons(plainSelects.get(i));
				curRoot=new Unionation(curRoot,right);
			}
		}else{
			System.out.println("Not legitimate sql query!");
		}
		return optimize(curRoot);
		//return curRoot;
	}
	
	private RAOperator productTreeCons(FromItem item,List<Join> joinItems){
		RAOperator curRoot=null;
		if(item instanceof Table){
			curRoot=new InputTable((Table)item,tableMap,dir,dbdir);
		}else if(item instanceof SubSelect){
			SelectBody stmt=((SubSelect) item).getSelectBody();
			String alias=((SubSelect)item).getAlias();
			curRoot=treeCons(stmt);
			curRoot.setAlias(alias);
		}else{
			//be empty now
		}
		for(int i=0;i<joinItems.size();i++){
			FromItem right=joinItems.get(i).getRightItem();
			if(right instanceof Table){
				curRoot=new CatesianProduct(curRoot,new InputTable((Table)right,tableMap,dir,dbdir));
				Expression on=joinItems.get(i).getOnExpression();
				if(on!=null)
					curRoot=new Selection(on,curRoot);
			}else if(right instanceof SubSelect){
				SelectBody stmt=((SubSelect) item).getSelectBody();
				String alias=((SubSelect)item).getAlias();
				RAOperator rig=treeCons(stmt);
				rig.setAlias(alias);
				curRoot=new CatesianProduct(curRoot,rig);
				Expression on=joinItems.get(i).getOnExpression();
				if(on!=null)
					curRoot=new Selection(on,curRoot);
			}
		}
		return curRoot;
	}
	
	private List<Function> findAggreFunctions(Expression havingCond,List<SelectItem> selectItems){
		List<Function> aggregates=new ArrayList<Function>();
		if(havingCond!=null){//there is a having conditon
		aggregates.addAll(findHelper(havingCond));
		}
		for(int i=0;i<selectItems.size();i++){
			if(selectItems.get(i) instanceof AllColumns||selectItems.get(i) instanceof AllTableColumns){
				//do nothing
			}else {//instanceof SelectExprssionItem
				SelectExpressionItem item=(SelectExpressionItem)(selectItems.get(i));
				aggregates.addAll(findHelper(item.getExpression()));
			}
		}
		return aggregates;
	}
	
	//this helper function finds the functions in exp which can appear in the having condition and select items
	private List<Function> findHelper(Expression exp){
		List<Function> functions=new ArrayList<Function>();
		if(exp instanceof Function){
			functions.add((Function)exp);
		}else if(exp instanceof Between){
			Expression leftExpression=((Between)exp).getLeftExpression();
			functions.addAll(findHelper(leftExpression));
		}else if(exp instanceof BinaryExpression){
			functions.addAll(findHelper(((BinaryExpression)exp).getLeftExpression()));
			functions.addAll(findHelper(((BinaryExpression)exp).getRightExpression()));
		}else if(exp instanceof InExpression){
			functions.addAll(findHelper(((InExpression)exp).getLeftExpression()));
		}
		return functions;
	}
	
	public static void printTree(RAOperator tree){
		if(tree==null)
			return;
		
		if(tree instanceof Selection && tree.getLeftChild() instanceof InputTable){
			System.out.println("Selection with condition:"+((Selection)tree).getCondition().toString()+" over "+((InputTable)tree.getLeftChild()).getTable().getName());
		}else{
			printTree(tree.getLeftChild());
			printTree(tree.getRightChild());
		}
		
		}
	
	
}
