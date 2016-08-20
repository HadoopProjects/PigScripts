//
// This code will validate the agruments called when invoked from a pig script
//

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class ValidArg extends EvalFunc<Long> {

	@Override
	public Long exec(Tuple input) throws IOException {
		int x = (int) input.get(0);
		int y = (int) input.get(1);
		return (long) Math.pow(x, y);
	}
	
	public Schema outputSchema(Schema input) {
		
		if(input.size()!=2 ){
			throw new RuntimeException("Expecting Two Arguments!");
		}
		
		try{
		if(input.getField(0).type != DataType.INTEGER || input.getField(1).type != DataType.INTEGER){
			throw new RuntimeException("Either of the fields is not of correct datatype. Expecting only integers!");
		}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return new Schema(new Schema.FieldSchema(null, DataType.LONG));
		
	}
}

