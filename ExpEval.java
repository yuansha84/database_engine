import edu.buffalo.cse562.*;
import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.*;

public class ExpEval extends Eval{

	//tuple info on which to evaluate the expression
	List<Column> tupleSchema;
	List<LeafValue> values;
	
	public ExpEval(List<Column> col,List<LeafValue> val){
		tupleSchema=col;
		values=val;
	}
	
	public void setTupleSchema(List<Column> schema){
		tupleSchema=schema;
	}
	
	public void setValues(List<LeafValue> val){
		values=val;
	}
	
	public LeafValue eval(Column col){
		int index=-1;
		if(col.getTable().getName()==null){
			for(int i=0;i<tupleSchema.size();i++){
				if(tupleSchema.get(i).getColumnName().equals(col.getColumnName())){
					index=i;
					break;
				}
			}
		}else{
			for(int i=0;i<tupleSchema.size();i++){
				if(tupleSchema.get(i).getWholeColumnName().equals(col.getWholeColumnName())){
					index=i;
					break;
				}
			}
		}
		return values.get(index);
	}
	
	/*public LeafValue leafCopy(LeafValue val){
		if(val instanceof LongValue)
			return new LongValue(((LongValue)val).getValue());
		else if(val instanceof DoubleValue)
			return new DoubleValue(((DoubleValue)val).getValue());
		else if(val instanceof )
	}*/
}
