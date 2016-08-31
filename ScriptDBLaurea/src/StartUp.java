
public class StartUp {

	public static void main(String[] args) {

		final String databaseName = "testDBNoSQL";
		
		//MongoDB.launch(databaseName);
		//System.out.println("FINE - MongoDB ");
		
		ApacheCassandra.launch(databaseName);
		System.out.println("FINE - ApacheCassandra ");	
		
		//ApacheHBase.launch(databaseName);
		//System.out.println("FINE - ApacheHBase ");		
	}	
}

