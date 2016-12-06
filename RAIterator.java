import java.util.*;
import net.sf.jsqlparser.expression.*;

public interface RAIterator {

	public void open();
	public List<LeafValue> getNext();
	public void close();
}
