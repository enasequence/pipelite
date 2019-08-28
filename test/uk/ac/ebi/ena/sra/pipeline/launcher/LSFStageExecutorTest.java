package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.io.IOException;
import java.nio.file.Files;

import org.junit.Assert;
import org.junit.Test;


import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.LSFExecutorConfig;
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


	@Test public void
	stageSpecificConfig() throws IOException
	{
		LSFStageExecutor se = new LSFStageExecutor( "TEST",
				new ResultTranslator( new ExecutionResult[] { new ExecutionResult() {
					@Override public RESULT_TYPE getType() { return null; }
					@Override public byte getExitCode() { return 0; }
					@Override public Class<? extends Throwable> getCause() { return null; }
					@Override public String getMessage() { return null; } } } ),

				null, "me", 1024, 0, 1, Files.createTempDirectory( "LSF-TEST-OUTPUT" ),
				"NOFILE", "NOPATH", new String[] { } );

		se.configure( new LSFExecutorConfig() {
			@Override public int getJavaMemoryLimit() { return  400; }
			@Override public int getLSFMemoryLimit() { return  2000; }
			@Override public int getLSFMemoryReservationTimeout() { return  14; }
			@Override public int getLSFCPUCores() { return  12; }
			@Override public String getLsfUser() { return "LSFUSER"; }
			@Override public String getLsfQueue() { return "LSFQUEUE"; }
			@Override public String[] getPropertiesPass() { return new String[] { }; }
		} );

		se.execute( new StageInstance()
		{
			{
				setEnabled( true );
			}
		} );

		String cmdl = se.get_info().getCommandline();
		Assert.assertTrue( cmdl.contains( " -M 2000 -R rusage[mem=2000:duration=14]" ) );
		Assert.assertTrue( cmdl.contains( " -Xmx400M" ) );
		Assert.assertTrue( cmdl.contains( " -n 12" ) );
		Assert.assertTrue( cmdl.contains( " -q LSFQUEUE" ) );
	}

	@Test public void
	genericConfig() throws IOException
	{
		LSFStageExecutor se = new LSFStageExecutor( "TEST",
				new ResultTranslator( new ExecutionResult[] { new ExecutionResult() {
					@Override public RESULT_TYPE getType() { return null; }
					@Override public byte getExitCode() { return 0; }
					@Override public Class<? extends Throwable> getCause() { return null; }
					@Override public String getMessage() { return null; } } } ),

				null, "me", 1024, 0, 6, Files.createTempDirectory( "LSF-TEST-OUTPUT" ),
				"NOFILE", "NOPATH", new String[] { } );

		se.configure( null );

		se.execute( new StageInstance()
		{
			{
				setEnabled( true );
			}
		} );

		String cmdl = se.get_info().getCommandline();
		Assert.assertTrue( cmdl.contains( " -M 1024 -R rusage[mem=1024:duration=0]" ) );
		Assert.assertTrue( !cmdl.contains( " -Xmx" ) );
		Assert.assertTrue( cmdl.contains( " -n 6 " ) );
	}
}
