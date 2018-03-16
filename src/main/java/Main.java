import java.io.File;
import java.io.IOException;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class Main {

	public static void main(String[] args) {

		String inputPath = new File("").getAbsolutePath().concat("/input/data.csv");
		String outputPath = new File("").getAbsolutePath().concat("/output");
		String schemaPath = new File("").getAbsolutePath().concat("/resources/schema.cql");

		String keyspace = "social_media";
		String column_family = "youtube";

		 SSTableWriter sstw = new SSTableWriter(inputPath
		 		 ,outputPath
				 ,keyspace
				 ,column_family
				 ,schemaPath);
		 try {
			sstw.writeSSTable();
		} catch (InvalidRequestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
