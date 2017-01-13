package cbdataimport;

import java.io.IOException;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class CBImportMainOptions {
	private int batchSize;
	private String RecFileName;
	private String cbNodesList; 
	private String cbBucket;
	private String cbBucketPassword;
		
	private Options options;
	
	private CommandLine commandLine;
		
	public CBImportMainOptions() throws ParseException, IOException
	{
		this.options = new Options();
		buildOptions(this.options);
	}
		
	public void init(String[] args) throws ParseException, IOException
	{
		CommandLineParser parser = new GnuParser();
        // parse the command line arguments
        this.commandLine = parser.parse( options, args );
        this.batchSize = Integer.parseInt(commandLine.getOptionValue("batch_size"));
       
        if (this.batchSize < 1 || this.batchSize > 1000000 ) {
        	System.out.println("Batch Size must be between 1 and 1000000.");
        	System.exit(-1);
        }
        
        this.RecFileName = commandLine.getOptionValue("rec_filename");
    	this.cbNodesList = commandLine.getOptionValue("cb_nodes");
    	this.cbBucket = commandLine.getOptionValue("cb_bucket");
    	this.cbBucketPassword = commandLine.getOptionValue("cb_password");
        
	}
	
	protected void buildOptions(Options options) {
		
		//Required Options
		Option batchSize = OptionBuilder.withArgName("Batch Size")
				.hasArg().withDescription("BatchSize. Must be between 1 and 1000000. ")
				.create("batch_size");
		batchSize.setRequired(true);
	
		Option mlaInqRecFile = OptionBuilder.withArgName("Record File")
				.hasArg().withDescription("Record File.")
				.create("rec_filename");
		mlaInqRecFile.setRequired(true);
		
		Option cbNodesList = OptionBuilder.withArgName("Node List")
				.hasArg().withDescription("List of Couchbase Servers (comma delimited).")
				.create("cb_nodes");
		cbNodesList.setRequired(true);
		
		Option cbBucketName = OptionBuilder.withArgName("Bucket Name")
				.hasArg().withDescription("Name of Bucket to Update.")
				.create("cb_bucket");

		cbBucketName.setRequired(true);

		Option cbBucketPassword = OptionBuilder.withArgName("Bucket Password")
				.hasArg().withDescription("Bucket Password.")
				.create("cb_password");
		
		cbBucketPassword.setRequired(true);
		
		options.addOption(cbBucketPassword);
		options.addOption(cbBucketName);
		options.addOption(cbNodesList);
		options.addOption(batchSize);
		
		options.addOption(mlaInqRecFile );
	}

	
	/**
	 * @return the options
	 */
	
	public Options getOptions() {
		return options;
	}
	
	public void printHelp(String title)
	{
		// automatically generate the help statement
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( title, options );
	}

	/**
	 * @return the commandLine
	 */
	public CommandLine getCommandLine() {
		return commandLine;
	}

	public StringBuffer displayParameters()
	{
		
		StringWriter stringWriter = new StringWriter();
		
		PrintWriter writer =new PrintWriter(stringWriter);
		
		writer.println("CBImportMainOptions Input Parameters");
		writer.println("Batch Size               :" + this.batchSize);
		writer.println("Records Filename:" + this.RecFileName);
		
		writer.println("Couchbase NodeList       :" + this.cbNodesList);  
		writer.println("Couchbase Bucket         :" + this.cbBucket);
		writer.println("Couchbase Password       :" + "************");
		
		writer.flush();
		writer.close();			
		
		return new StringBuffer(stringWriter.toString());		
		 
	}
	
	public String getMLAInqRecFilename() {
		return this.RecFileName;
	}
		
	public int getBatchSize() {
		return this.batchSize; 
	}
	
	public String getCBNodesList() {
		return this.cbNodesList;
	}
	
	public String getCBBucket() {
		return this.cbBucket; 
	}
	
	public String getCBBucketPassword(){
		return this.cbBucketPassword;
	}
	
}
