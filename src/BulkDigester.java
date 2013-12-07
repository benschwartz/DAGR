

public class BulkDigester {

	public static void main(String[] args) {
		if(Utils.connect()){
			Utils.disconnnect();
		}
		else{
			throw new RuntimeException();
		}
	}

}
