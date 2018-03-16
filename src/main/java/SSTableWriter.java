import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class SSTableWriter {
	
	private String IN;
	private String OUT;
	private String SCHEMA;
	private String keyspace;
	private String column_family;

	public SSTableWriter(String csv_dir, 
							String sstable_Dir,
								String keyspace,
									String column_family,
										String schema_dir) {
		this.IN = csv_dir;
        this.OUT = sstable_Dir + File.separator + keyspace + File.separator + column_family ;
		new File(OUT).mkdirs();
		this.SCHEMA = readSchemaFile(schema_dir);
		this.keyspace = keyspace;
		this.column_family = column_family;
	}

	public void writeSSTable() throws InvalidRequestException, IOException {
		
		Config.setClientMode(true);
		
		String INSERT_STMT = getInsertStatement(this.keyspace, this.column_family);
			   
		CQLSSTableWriter writer = CQLSSTableWriter.builder()
								// set output directory
									.inDirectory(this.OUT)
								// set target schema
										.forTable(SCHEMA)
								// set CQL statement to insert data
											.using(INSERT_STMT)
								// set partitioner if needed
												.withPartitioner(new Murmur3Partitioner())
													.build();

		
//		read CSV file
		CsvParser csvparser = new CsvParser(new CsvParserSettings());
		csvparser.beginParsing(new File(this.IN));
		
		Object[] row;
		
//		paralellize
        System.out.println("Parsing commenced!");
		while ((row = csvparser.parseNext()) != null) {
			writer.addRow(row);
		}
        System.out.println("Parsing completed!");
		writer.close();
	}
	
	private String readSchemaFile(String schema_dir) {
		File SchemaFile;
		FileInputStream fis;
		byte[] data = null;
		
		try {
			SchemaFile = new File(schema_dir);
			fis = new FileInputStream(SchemaFile);
			data = new byte[(int) SchemaFile.length()];
			fis.read(data);	
			fis.close();
		}catch(IOException e) {
			System.err.println("Column family schema definition directory invalid!");
			e.printStackTrace();
		}
		return new String(data, Charset.defaultCharset());
	}	
	
	private String getInsertStatement(String keyspace,String column_family) {
		
		return String.format("INSERT INTO %S.%S("
				+"VIDEO_ID,TRENDING_DATE,TITLE,CHANNEL_TITLE,CATEGORY_ID,PUBLISH_TIME,"
				+"TAGS,VIEWS,LIKES,DISLIKES,COMMENT_COUNT,THUMBNAIL_LINK,COMMENTS_DISABLED,RATINGS_DISABLED,"
				+"VIDEO_ERROR_OR_REMOVED,DESCRIPTION"
				+ ") VALUES ("
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ")", keyspace, column_family);
	}

	
}
