//
// This class will find intersection between two bags
//
// Input:  ({(rahul),(kumar),(sahukar)}    {(rahul),(dravid),(winter),(kumar)})
// Output: ({(kumar,rahul)})
//

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class Intersect extends EvalFunc<DataBag> {

	private static final BagFactory bagFactory = BagFactory.getInstance();

	@Override
	public DataBag exec(Tuple input) throws IOException {

		DataBag opBag = bagFactory.newDefaultBag();

		Set<String> setOne = new HashSet<String>();
		Set<String> setTwo = new HashSet<String>();

		DataBag one = (DataBag) input.get(0);
		DataBag two = (DataBag) input.get(1);

		for (Tuple tuple : one) {
			setOne.add((String) tuple.get(0));
		}
		for (Tuple tuple : two) {
			setTwo.add((String) tuple.get(0));
		}

		setOne.retainAll(setTwo);

		Tuple t = TupleFactory.getInstance().newTuple();
		for (String item : setOne) {
			t.append(item);
		}
		opBag.add(t);

		return opBag;
	}

}
