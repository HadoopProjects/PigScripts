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
		if(input.size()!=2 ){
			throw new RuntimeException("Expecting Two Arguments!");
		}
		
		try{
			if ((input.getField(0).type != DataType.DOUBLE && input.getField(0).type != DataType.INTEGER)
					|| input.getField(1).type != DataType.INTEGER){
			throw new RuntimeException("Either of the fields is not of correct datatype! >>> " + input.getField(0).type +" " + input.getField(1).type );
		}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return new Schema(new Schema.FieldSchema(null, DataType.LONG));
	}
	
	public List<FuncSpec> getArgToFuncMapping(){
		
		List<FuncSpec> list = new ArrayList<FuncSpec>();
		
		Schema s = new Schema();
		s.add(new Schema.FieldSchema(null, DataType.INTEGER));
		s.add(new Schema.FieldSchema(null, DataType.INTEGER));
		list.add(new FuncSpec(this.getClass().getName(), s));
		
		s = new Schema();
		s.add(new Schema.FieldSchema(null, DataType.DOUBLE));
		s.add(new Schema.FieldSchema(null, DataType.INTEGER));
		list.add(new FuncSpec(this.getClass().getName(), s));
		
		return list;
		
	}
	
}

