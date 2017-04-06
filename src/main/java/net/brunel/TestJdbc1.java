package net.brunel;

import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowRuntimeProps;
import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.jdbc.TableDesc;
import cascading.jdbc.db.DBInputFormat;
import cascading.jdbc.db.DBOutputFormat;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.operation.Identity;

/**
 * This is a logical copy of 
 * https://github.com/Cascading/cascading-jdbc/blob/3.0/cascading-jdbc-core/src/test/java/cascading/jdbc/JDBCTestingBase.java
 * 
 * @author Andre Busche
 */
public class TestJdbc1 {

	private static final String DB_PORT = "32768";

	private static final String DB_HOST = "172.16.102.85";

	private static final String DB_PASSWORD = "password";

	private static final String DB_USERNAME = "postgres";

	private static final String TESTING_TABLE_NAME = "cascadingtest";

	protected static Class<? extends DBInputFormat> inputFormatClass = DBInputFormat.class;

	/** the JDBC url for the tests. subclasses have to set this */
	protected static String jdbcurl = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/postgres";

	/** the name of the JDBC driver to use. */
	protected static String driverName = "org.postgresql.ds.PGConnectionPoolDataSource";

	public static void main(String[] args) throws IOException {
		String inputFile = "small.txt";

		/*
		 * step 1: upload the contents from small.txt to a database
		 */
		Tap<?, ?, ?> source = new Hfs(new TextLine(), inputFile);
		Fields fields = new Fields(new Comparable[] { "num", "lwr", "upr" },
				new Type[] { int.class, String.class, String.class });
		Pipe parsePipe = new Each("insert", new Fields("line"), new RegexSplitter(fields, "\\s"));

		String[] columnNames = { "num", "lwr", "upr" };
		String[] columnDefs = { "INT NOT NULL", "VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL" };
		String[] primaryKeys = { "num", "lwr" };
		TableDesc tableDesc = getNewTableDesc(TESTING_TABLE_NAME, columnNames, columnDefs, primaryKeys);

		JDBCScheme scheme = getNewJDBCScheme(fields, columnNames);

		JDBCTap replaceTap = getNewJDBCTap(tableDesc, scheme, SinkMode.REPLACE);
		// forcing commits to test the batch behaviour
		replaceTap.setBatchSize(2);

		FlowConnector connector = createFlowConnector(createProperties());

		/*
		 *  create a flow from the parsePipe.
		 *  
		 *  parsePipe defined a source named 'insert' which is plumbed to source, and 
		 *  outputs its results to the replaceTap 
		 */
		FlowDef flowDef = createFlow(source, "insert", parsePipe, replaceTap);
		Flow<?> parseFlow = connector.connect( flowDef );
		
		parseFlow.complete();

		// verify that 13 lines were processed.
		verifySink(parseFlow, 13);

		/*
		 * step 2: read data from the file, and write it to the file
		 * 
		 * The sink is stored in Hfs, relative to the current users' working directory
		 */
		Tap<?, ?, ?> copiedFile = new Hfs(new TextLine(), "build/test/jdbc", SinkMode.REPLACE);

		// simple pipe which does an identity transformation
		Pipe copyPipe = new Each("read", new Identity());

		// Here, plumb the source to the named source 'read' and copy to the sink.
		FlowDef copyFlowDef = createFlow(source, "read", copyPipe, copiedFile);
		Flow copyFlow = connector.connect(copyFlowDef);

		copyFlow.complete();

		// verify that 13 records were processed.
		verifySink(copyFlow, 13);

		/*
		 * step 3: read data from the copied text file from step 2 and update (not insert) the data to the database 
		 */

		JDBCScheme jdbcScheme = getNewJDBCScheme(columnNames, null, new String[] { "num", "lwr" });
		jdbcScheme.setSinkFields(fields);
		Tap<?, ?, ?> updateTap = getNewJDBCTap(tableDesc, jdbcScheme, SinkMode.UPDATE);

		/*
		 * use the parsePipe of step 1 as the pipe, which defined the source by the name 'insert'
		 * Use the copiedFile tap as the source of data and write the data to the updateTap.
		 */
		FlowDef copyFlowDef2 = createFlow(copiedFile, "insert", parsePipe, updateTap);
		Flow copyFlow2 = connector.connect(copyFlowDef2);

		copyFlow2.complete();
		copyFlow2.cleanup();

		// verify that 13 lines were processed.
		verifySink(copyFlow2, 13);
		
		/*
		 * step 4: read data from the table in to a text file using a custom query
		 */

		Tap<?, ?, ?> sourceTap = getNewJDBCTap(getNewJDBCScheme(columnNames,
				String.format("select num, lwr, upr from %s %s", TESTING_TABLE_NAME, TESTING_TABLE_NAME),
				"select count(*) from " + TESTING_TABLE_NAME));

		/*
		 * read from the source using the copyPipe from step 2 and write the contents to copiedFile from step 2.
		 */
		FlowDef readFlowDef = createFlow(sourceTap, "read", copyPipe, copiedFile);
		Flow readFlow = connector.connect(readFlowDef);
		
		readFlow.complete();

		verifySink(readFlow, 13);
	}

	private static FlowDef createFlow(Tap<?, ?, ?> source,  String sourceIdentifier, Pipe processingPipe, Tap<?,?,?> target) {
		FlowDef flowDef = FlowDef.flowDef().setName( "import" );
		// add all libraries
		// TODO: This checks for a path as also used in the gradle.build file. This is a redundant check!
		flowDef = addLibraries(flowDef, "libs/");
		
		flowDef = flowDef
				.addSource( sourceIdentifier, source )
				.addTailSink( processingPipe, target );
		return flowDef;
	}

	private static void verifySink(Flow<?> flow, int expects) throws IOException {
		int count = 0;

		TupleEntryIterator iterator = flow.openSink();

		while (iterator.hasNext()) {
			count++;
			iterator.next();
		}

		iterator.close();

		System.out.println("number of values expected: " + expects + " and count is " + count);
	}


	protected static TableDesc getNewTableDesc(String tableName, String[] columnNames, String[] columnDefs,
			String[] primaryKeys) {
		return new TableDesc(tableName, columnNames, columnDefs, primaryKeys);
	}

	protected static JDBCScheme getNewJDBCScheme(Fields fields, String[] columnNames) {
		return new JDBCScheme(inputFormatClass, fields, columnNames);
	}

	protected static JDBCScheme getNewJDBCScheme(String[] columns, String[] orderBy, String[] updateBy) {
		return new JDBCScheme(inputFormatClass, DBOutputFormat.class, columns, orderBy, updateBy);
	}

	protected static JDBCScheme getNewJDBCScheme(String[] columnNames, String contentsQuery, String countStarQuery) {
		return new JDBCScheme(inputFormatClass, new Fields(columnNames), columnNames, contentsQuery, countStarQuery,
				-1);
	}

	protected static JDBCTap getNewJDBCTap(TableDesc tableDesc, JDBCScheme jdbcScheme, SinkMode sinkMode) {
		return new JDBCTap(jdbcurl, DB_USERNAME, DB_PASSWORD, driverName, tableDesc, jdbcScheme, sinkMode);
	}

	protected static JDBCTap getNewJDBCTap(JDBCScheme jdbcScheme) {
		return new JDBCTap(jdbcurl, DB_USERNAME, DB_PASSWORD, driverName, jdbcScheme);
	}

	protected static Properties createProperties() {
		Properties props = new Properties();
		props.setProperty("mapred.reduce.tasks.speculative.execution", "false");
		props.setProperty("mapred.map.tasks.speculative.execution", "false");
		AppProps.setApplicationJarClass(props, TestJdbc1.class);
		AppProps.setApplicationName(props, TestJdbc1.class.getName());
		props.setProperty(FlowRuntimeProps.GATHER_PARTITIONS, "1");
		return props;
	}

	protected static FlowConnector createFlowConnector(final Map<Object, Object> properties) {
		return new Hadoop2TezFlowConnector(properties);
	}

	public static FlowDef addLibraries(FlowDef flowDef, String librariesFolder) {
		File libraries = new File(librariesFolder);
		System.out.println("Listing files from " + libraries.getAbsolutePath());
		for (File name : libraries.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".jar");
			}
		})) {
			System.out.println("Adding " + name);
			flowDef = flowDef.addToClassPath(name.getAbsolutePath());
		}
		return flowDef;
	}

}
