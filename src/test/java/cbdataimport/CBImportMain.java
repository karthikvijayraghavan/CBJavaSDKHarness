package cbdataimport;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import rx.Observable;
import rx.functions.Func1;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.subdoc.DocumentFragment;

public class CBImportMain {
	
	private static int READ_BUFFER_SIZE  = 8192 * 4;
	
	private final static long TIMEOUT = 10000000;
	
	static Cluster mainCluster;
	static Bucket mainBucket;

	static long totalReadRecords = 0;
	static long totalFalures = 0;
		
	static long totalUpdates = 0;
	static long totalFailedUpdates = 0;
	
	
	static long totalFailedInserts = 0;
	static long totalFailedExists = 0;
	
	static long totalFormatErrors = 0; 
	
	
	//Exceptions...
	static long totalCouchbaseErrors = 0;
	
	
	static public String checkNull(String inputValue) {
		if (inputValue == null) {
			return "";
		} else {
			return inputValue; 
		}
	}
	
	static public String buildKey(String first, String second, String third) {
		
		StringBuffer KeyVal = new StringBuffer();
		
		String firstValue = "";
		String secondValue = "";
		String ThirdValue = "";
		
		if (first != null) {
			firstValue = first;
		}
		
		if (second != null) {
			secondValue = second;
		}
		
		if (third != null) {
			ThirdValue = third;
		}
				
		KeyVal.append("key::");
		KeyVal.append(firstValue);
		KeyVal.append(",");
		KeyVal.append(secondValue);
		KeyVal.append(",");
		KeyVal.append(ThirdValue);
		
		return KeyVal.toString();
	}
	
	static void bulkDocumentsUpdate(final List<String> RecordsList) {
		
		//bulk code comes here...
		try {
			
			try {
				
				 List<CBOperationResult> results = Observable.from(RecordsList).map(  
						 
						 new Func1<String, CBOperationResult>() {

							 public CBOperationResult call(String inputRecord) {
								 
									String[] Fields = StringUtils.splitPreserveAllTokens(inputRecord, "|");
									
									CBOperationResult opRecord  = null;
																		
									if (Fields.length >= 15) {
										
										//add a routine that checks for null values.
										String first = checkNull(Fields[4]);
										String second = checkNull(Fields[5]);
										String third = checkNull(Fields[6].toUpperCase());
										
										String keyValue = buildKey(first, second, third);										
										
										if (first.isEmpty() || second.isEmpty() || third.isEmpty()) {
											
											//Format error 
											opRecord = new CBOperationResult(keyValue, inputRecord, null, null, CBOperationResultStatus.FORMAT_ERROR);
											
										} else {
											
											JsonObject RecJson = 
													JsonObject.create().
													put("1", "1"). 
													put("2", checkNull(Fields[1])).
													put("3", checkNull(Fields[2])).
													put("4", checkNull(Fields[3])).
													put("5", checkNull(Fields[4])).
													put("6", checkNull(Fields[5])).
													put("7", checkNull(Fields[6])).
													put("8", checkNull(Fields[7])).
													put("9", checkNull(Fields[8])).
													put("10", checkNull(Fields[9])).
													put("11", checkNull(Fields[10])).
													put("12", checkNull(Fields[11])).
													put("13", checkNull(Fields[12])).
													put("14", checkNull(Fields[13])).
													put("15", checkNull(Fields[14]));
																						
											opRecord = new CBOperationResult(keyValue, inputRecord, RecJson , null, CBOperationResultStatus.DATA_PARSED);
											
										}
																			
									} else {
										//format error
										opRecord = new CBOperationResult("invalidkey", inputRecord, null, null, CBOperationResultStatus.FORMAT_ERROR);
									}
																		
									return opRecord;
								}
						}
				    )
				    
				    .flatMap(new Func1<CBOperationResult, Observable<CBOperationResult>>() {
				    	
						public Observable<CBOperationResult> call(final CBOperationResult op) {
							
							if (op.getStatus() == CBOperationResultStatus.DATA_PARSED ) {
									
									return mainBucket.async().exists(op.getId()) 
											.map(new Func1<Boolean, CBOperationResult>() {
												
										public CBOperationResult call(Boolean existFlag) {
											if (existFlag == true) {
											    op.setStatus(CBOperationResultStatus.EXIST_ALREADY_FOUND);
											} else {
												op.setStatus(CBOperationResultStatus.EXIST_NOT_FOUND);
											}
											return op;
										}
									}).onErrorResumeNext(new Func1<Throwable, Observable<CBOperationResult>>() {
										public Observable<CBOperationResult> call(Throwable e) {
											op.setError(e);
											op.setStatus( CBOperationResultStatus.EXIST_FAILURE);
											return Observable.just(op);
										}
									}); 
							}
							else 
							{
								return Observable.just(op);
							}
						} 
					 })
				    
				    .flatMap(new Func1<CBOperationResult, Observable<CBOperationResult>>() {
				    	
						public Observable<CBOperationResult> call(final CBOperationResult op) {
							
							if (op.getStatus() == CBOperationResultStatus.EXIST_NOT_FOUND ||
							op.getStatus() == CBOperationResultStatus.DATA_PARSED ) {
								
									JsonArray emptyArray = JsonArray.create();
									
									//JsonObject docObject = JsonObject.create().put(DocumentConstants.DOC_UPDATED_TAG, UTCTimeStamp).put(DocumentConstants.DOC_REC_ARRARY_TAG, emptyArray);
									JsonObject docObject = JsonObject.create().put("recs", emptyArray);
									JsonDocument insertDoc = JsonDocument.create(op.getId(), docObject); 
									
									return mainBucket.async().insert(insertDoc)
											.map(new Func1<JsonDocument, CBOperationResult>() {
												
										public CBOperationResult call(JsonDocument doc) {
											//op.setResult(doc);
											op.setStatus(CBOperationResultStatus.INSERT_SUCCESS);
											return op;
										}
										
									}).onErrorResumeNext(new Func1<Throwable, Observable<CBOperationResult>>() {
										public Observable<CBOperationResult> call(Throwable e) {
											op.setError(e);
											if (e instanceof DocumentAlreadyExistsException) {
												op.setStatus( CBOperationResultStatus.INSERT_ALREADYEXISTS );
											} else { 
												op.setStatus( CBOperationResultStatus.INSERT_FAILURE );
											}
											return Observable.just(op);
										}
									}); 
							}
							else 
							{
								return Observable.just(op);
							}
						} 
					    })
				    
				    .flatMap(
				    				    		
				        	new Func1<CBOperationResult, Observable<CBOperationResult>>() {
					
				        	public Observable<CBOperationResult> call(final CBOperationResult op) {
										        		
				        		if( op.getStatus() == CBOperationResultStatus.EXIST_ALREADY_FOUND ||
				        			op.getStatus() == CBOperationResultStatus.INSERT_SUCCESS ||
				        			op.getStatus() == CBOperationResultStatus.INSERT_ALREADYEXISTS )
				        		{
				        			//return mainBucket.async().mutateIn(op.getId()).upsert(DocumentConstants.DOC_UPDATED_TAG, UTCTimeStamp, false)
				        				
				        			return mainBucket.async().mutateIn(op.getId())		        					
				        				   .arrayAppend("recs", op.getResult(), false).execute()
				        				   .map(
				        					new Func1<DocumentFragment<Mutation>, CBOperationResult>() {
				        						public CBOperationResult call(DocumentFragment<Mutation> doc) {
				        							//Success and pass thru!  
				        							op.setStatus(CBOperationResultStatus.UPSERT_SUCCESS);
				        							return op;
				        				   }
									
									       }).onErrorReturn(new Func1<Throwable, CBOperationResult>() {
									    	   public CBOperationResult call(Throwable e) {
									    		   op.setError(e);
									    		   op.setStatus(CBOperationResultStatus.UPSERT_FAILURE );
									    		   return op;
									       }
									});
		                } 
		                else {	
		                	return Observable.just(op);
			                
		                }
		                
					} // close of function
				}
				
				).toList().toBlocking().single();
				 
				
				 
				for( CBOperationResult op: results ) {
					
					if (op.getStatus() == CBOperationResultStatus.UPSERT_SUCCESS ) {
						totalUpdates++;					
					}
					
					if (op.getStatus() == CBOperationResultStatus.UPSERT_FAILURE ) {
						if ( op.getError() instanceof CouchbaseException ) {
							System.err.println("ERROR:" + op.getId() + " UPDATE FAILED DUE TO COUCHBASE. " + op.getError().getMessage());
							System.err.println("UPSERTFAILEDRECORD:" + op.getRecord());
							totalFailedUpdates++;
						}
					}
					
					if (op.getStatus() == CBOperationResultStatus.FORMAT_ERROR) {
						System.err.println("ERROR:" + op.getId() + " DATA FORMAT ERROR.");
						System.err.println("FORMATERRORRECORD:" + op.getRecord());
						totalFormatErrors++;
					} 
					
					if (op.getStatus() == CBOperationResultStatus.INSERT_FAILURE) {
						System.err.println("ERROR:" + op.getId() + " FAILED To INSERT A NEW RECORD. " + op.getError().getMessage());
						System.err.println("INSERTFAILRECORD:" + op.getRecord());
						totalFailedInserts++;
					}
					
					if (op.getStatus() == CBOperationResultStatus.EXIST_FAILURE) {
						System.err.println("ERROR:" + op.getId() + " FAILED To CHECK TO SEE IF RECORD EXISTS. " + op.getError().getMessage());
						totalFailedExists++;
					}
					
				}
				
			} catch (CouchbaseException ex) {
				ex.printStackTrace();
				System.exit(1);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(1);
		}	
	}
	
	public static void main(String[] args) throws Exception {
			
		CBImportMainOptions options = new CBImportMainOptions();
				
		if (args == null || args.length == 0) {
			options.printHelp(CBImportMain.class.getSimpleName());
			return;
		}
		
		options.init(args);
		
		System.out.println(options.displayParameters().toString());
				
		totalReadRecords = 0;
		
		totalFalures = 0;
		totalUpdates = 0;
		totalFailedUpdates = 0;
		totalFailedInserts = 0;
		totalFormatErrors = 0; 
		totalFailedExists = 0;		
		totalCouchbaseErrors = 0;
		
		try {
						
			CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
					//.kvEndpoints(64)
					//.ioPoolSize(64)
					//.computationPoolSize(64)
					//.requestBufferSize(131072)
					//.responseBufferSize(131072)
					.build();
						
			mainCluster = CouchbaseCluster.create(env, options.getCBNodesList());
		
			mainBucket = mainCluster.openBucket(options.getCBBucket(), options.getCBBucketPassword(), TIMEOUT, TimeUnit.MILLISECONDS);
						
			try {
				
				long startTime = System.currentTimeMillis();
			
				BufferedReader FileReader = new BufferedReader(new FileReader(options.getMLAInqRecFilename()), READ_BUFFER_SIZE);
				
				String RecordLine = "";
								
				List<String> RecordsList = new ArrayList<String>();
				
				while ((RecordLine = FileReader.readLine()) != null) {
					
					totalReadRecords++;
					
					RecordsList.add(RecordLine);
					
					try {
						
						if (totalReadRecords % options.getBatchSize() == 0) {
							bulkDocumentsUpdate(RecordsList); 
							RecordsList.clear();
						}
						
						if ( totalReadRecords % 10000 == 0 ) {
							System.out.println("Completed " + totalReadRecords + " records.");
						}
						
					} catch (CouchbaseException ex) {
						ex.printStackTrace();
						System.exit(1);
					}
				}
								
				if ( RecordsList.size() > 0 ) {
					bulkDocumentsUpdate(RecordsList);
					RecordsList.clear();
				}
				
				FileReader.close();
				
				
				long endTime = System.currentTimeMillis();

	            long execTime = (endTime - startTime);
	        	
				StringBuffer statBuffer = new StringBuffer();
				
				statBuffer.append("Data Ingestion\n");
				statBuffer.append(String.format(" Number Of Records Read from Input File      : %d\n", totalReadRecords));
				statBuffer.append(String.format(" Number Of Records Updated                   : %d\n", totalUpdates));
				
				statBuffer.append(String.format(" Number Of Records Update Failures           : %d\n", totalFailedUpdates));
				statBuffer.append(String.format(" Number Of Records Insert Failures           : %d\n", totalFailedInserts));
				statBuffer.append(String.format(" Number Of Records Exists Call Failures      : %d\n", totalFailedExists));				
				
				statBuffer.append(String.format(" Number Of Records      Record Format Error  : %d\n", totalFormatErrors));
				
				statBuffer.append("\n");
				
				statBuffer.append(String.format(" Total Exec Time(ms)                         : %d\n", execTime));
				
				System.err.println( statBuffer.toString());
			
				//mainBucket.close();
				mainCluster.disconnect();
				
				mainCluster = null;
				
				//env.shutdownAsync().toBlocking().single();
								
				//env = null;
				
				System.err.println("Successfully disconnected from Couchbase cluster.");
				
				if (totalFailedUpdates > 0 ) {
					System.err.println("Please check standard error for eror messages!!!!!"); 
					System.exit(1);
				} else {
					System.exit(0);
				}
				
			} catch (CouchbaseException couchEx) {
				couchEx.printStackTrace();
				System.exit(1);
			}
			
			
		} catch (Exception ex) {
			ex.printStackTrace();
			
		}

	}
	
}

