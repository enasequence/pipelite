package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.io.IOException;
import java.nio.file.Files;

import org.junit.Assert;
import org.junit.Test;


import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;

public class 
LSFStageExecutorTest 
{
	@Test public void
	testNoQueue() throws IOException
	{
		LSFStageExecutor se = new LSFStageExecutor( "TEST", 
													new ResultTranslator( new ExecutionResult[] { new ExecutionResult() {
																		  @Override public RESULT_TYPE getType() { return null; }
																		  @Override public byte getExitCode() { return 0; }
																		  @Override public Class<? extends Throwable> getCause() { return null; }
																		  @Override public String getMessage() { return null; } } } ),
													
													null, "me", 1024, 0, 1, Files.createTempDirectory( "LSF-TEST-OUTPUT" ),
													"NOFILE", "NOPATH", new String[] { } );
		
		se.execute( new StageInstance() 
		{ 
			{ 
				setEnabled( true ); 
			} 
		} );
		
		
		Assert.assertFalse( se.get_info().getCommandline().contains( "-q " ) );
		
	}	


	@Test public void
	testQueue() throws IOException
	{
		LSFStageExecutor se = new LSFStageExecutor( "TEST", 
													new ResultTranslator( new ExecutionResult[] { new ExecutionResult() {
																		  @Override public RESULT_TYPE getType() { return null; }
																		  @Override public byte getExitCode() { return 0; }
																		  @Override public Class<? extends Throwable> getCause() { return null; }
																		  @Override public String getMessage() { return null; } } } ),
													
													"queue", "me", 1024, 0, 1, Files.createTempDirectory( "LSF-TEST-OUTPUT" ),
													"NOFILE", "NOPATH", new String[] { } );
		
		se.execute( new StageInstance() 
		{ 
			{ 
				setEnabled( true ); 
			} 
		} );
		
		
		Assert.assertTrue( se.get_info().getCommandline().contains( "-q queue" ) );
		
	}	
}
