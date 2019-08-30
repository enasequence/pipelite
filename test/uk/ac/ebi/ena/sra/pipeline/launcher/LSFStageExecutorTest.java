package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.io.IOException;
import java.nio.file.Files;
import org.junit.Assert;
import org.junit.Test;
import uk.ac.ebi.ena.sra.pipeline.executors.LSFExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;

public class 
LSFStageExecutorTest 
{
	private ResultTranslator
	makeResultTranslator()
	{
		return new ResultTranslator( new ExecutionResult[] { new ExecutionResult() {
			@Override public RESULT_TYPE getType() { return null; }
			@Override public byte getExitCode() { return 0; }
			@Override public Class<? extends Throwable> getCause() { return null; }
			@Override public String getMessage() { return null; } } } );
	}


	private LSFExecutorConfig
	makeDefaultConfig() throws IOException
	{
		String tmpd_def = Files.createTempDirectory("LSF-TEST-OUTPUT-DEF").toString();

		return new LSFExecutorConfig() {
			@Override public int getLSFMemoryLimit() { return  1024; }
			@Override public int getLSFMemoryReservationTimeout() { return 9; }
			@Override public int getLSFCPUCores() { return  6; }
			@Override public String getLsfUser() { return "LSFUSER"; }
			@Override public String getLsfQueue() { return "LSFQUEUE"; }
			@Override public String getLsfOutputPath() { return tmpd_def; }
		};
	}


	private StageInstance
	makeDefaultStageInstance()
	{
		return new StageInstance() {
			{
				setEnabled(true);
				setPropertiesPass(new String[]{});
			}
		};
	}


	@Test public void
	testNoQueue() throws IOException
	{
		ResultTranslator translator = makeResultTranslator();

		String tmpd_def = Files.createTempDirectory("LSF-TEST-OUTPUT-DEF").toString();
		LSFExecutorConfig cfg_def = new LSFExecutorConfig() {
			@Override public int getLSFMemoryLimit() { return  1024; }
			@Override public int getLSFMemoryReservationTimeout() { return 9; }
			@Override public int getLSFCPUCores() { return  6; }
			@Override public String getLsfUser() { return "LSFUSER"; }
			@Override public String getLsfQueue() { return null; }
			@Override public String getLsfOutputPath() { return tmpd_def; }
		};

		LSFStageExecutor se = new LSFStageExecutor( "TEST", translator,
				"NOFILE", "NOPATH", new String[] { }, cfg_def );
		
		se.execute( makeDefaultStageInstance() );
		
		
		Assert.assertFalse( se.get_info().getCommandline().contains( "-q " ) );
		
	}	


	@Test public void
	testQueue() throws IOException
	{
		ResultTranslator translator = makeResultTranslator();

		String tmpd_def = Files.createTempDirectory("LSF-TEST-OUTPUT-DEF").toString();
		LSFExecutorConfig cfg_def = new LSFExecutorConfig() {
			@Override public int getLSFMemoryLimit() { return  1024; }
			@Override public int getLSFMemoryReservationTimeout() { return 9; }
			@Override public int getLSFCPUCores() { return  6; }
			@Override public String getLsfUser() { return "LSFUSER"; }
			@Override public String getLsfQueue() { return "queue"; }
			@Override public String getLsfOutputPath() { return tmpd_def; }
		};

		LSFStageExecutor se = new LSFStageExecutor( "TEST", translator,
				"NOFILE", "NOPATH", new String[] { }, cfg_def );
		
		se.execute( makeDefaultStageInstance() );


		Assert.assertTrue( se.get_info().getCommandline().contains( "-q queue" ) );
	}


	@Test public void
	stageSpecificConfig() throws IOException
	{
		ResultTranslator translator = makeResultTranslator();
		LSFExecutorConfig cfg_def = makeDefaultConfig();
		LSFStageExecutor se = new LSFStageExecutor( "TEST", translator,
				"NOFILE", "NOPATH", new String[] { }, cfg_def );

		String tmpd_stg = Files.createTempDirectory("LSF-TEST-OUTPUT-STG").toAbsolutePath().toString();
		System.out.println( tmpd_stg );
		LSFExecutorConfig cfg_stg = new LSFExecutorConfig() {
			@Override public int getLSFMemoryLimit() { return  2000; }
			@Override public int getLSFMemoryReservationTimeout() { return  14; }
			@Override public int getLSFCPUCores() { return  12; }
			@Override public String getLsfUser() { return "LSFUSER"; }
			@Override public String getLsfQueue() { return "LSFQUEUE"; }
			@Override public String getLsfOutputPath() { return tmpd_stg; }
		};

		se.configure( cfg_stg );

		se.execute( makeDefaultStageInstance() );

		String cmdl = se.get_info().getCommandline();
		Assert.assertTrue( cmdl.contains( " -M 2000 -R rusage[mem=2000:duration=14]" ) );
		Assert.assertTrue( cmdl.contains( " -n 12" ) );
		Assert.assertTrue( cmdl.contains( " -q LSFQUEUE" ) );
		Assert.assertTrue( cmdl.contains( " -oo " + tmpd_stg + "\\" ) );
		Assert.assertTrue( cmdl.contains( " -eo " + tmpd_stg + "\\" ) );
	}


	@Test public void
	genericConfig() throws IOException
	{
		ResultTranslator translator = makeResultTranslator();
		LSFExecutorConfig cfg_def = makeDefaultConfig();
		LSFStageExecutor se = new LSFStageExecutor( "TEST", translator,
				"NOFILE", "NOPATH", new String[] { }, cfg_def );

		se.configure( null );

		se.execute( makeDefaultStageInstance() );

		String cmdl = se.get_info().getCommandline();
		Assert.assertTrue( cmdl.contains( " -M 1024 -R rusage[mem=1024:duration=9]" ) );
		Assert.assertTrue( cmdl.contains( " -n 6 " ) );
	}


	@Test public void
	javaMemory() throws IOException
	{
		ResultTranslator translator = makeResultTranslator();
		LSFExecutorConfig cfg_def = makeDefaultConfig();
		LSFStageExecutor se = new LSFStageExecutor( "TEST", translator,
				"NOFILE", "NOPATH", new String[] { }, cfg_def );

		se.configure( null );

		se.execute( new StageInstance()
		{
			{
				setEnabled( true );
				setPropertiesPass( new String[] { } );
				setJavaMemoryLimit( 2000 );
			}
		} );

		String cmdl = se.get_info().getCommandline();
		Assert.assertTrue( cmdl.contains( " -Xmx2000M" ) );
	}


	@Test public void
	propertiesPassStageSpecific() throws IOException
	{
		ResultTranslator translator = makeResultTranslator();
		LSFExecutorConfig cfg_def = makeDefaultConfig();
		LSFStageExecutor se = new LSFStageExecutor( "TEST", translator,
				"NOFILE", "NOPATH", new String[] { "user.dir" }, cfg_def );

		se.configure( null );

		se.execute( new StageInstance()
		{
			{
				setEnabled( true );
				setPropertiesPass( new String [] { "user.country" } );
			}
		} );

		String cmdl = se.get_info().getCommandline();
		Assert.assertTrue( cmdl.contains( " -Duser.country=" ) );
		Assert.assertTrue( cmdl.contains( " -Duser.dir=" ) );
	}


	@Test public void
	propertiesPassGeneric() throws IOException
	{
		ResultTranslator translator = makeResultTranslator();
		LSFExecutorConfig cfg_def = makeDefaultConfig();
		LSFStageExecutor se = new LSFStageExecutor( "TEST", translator,
				"NOFILE", "NOPATH", new String[] { "user.dir" }, cfg_def );

		se.configure( null );

		se.execute( makeDefaultStageInstance() );

		String cmdl = se.get_info().getCommandline();
		Assert.assertTrue( cmdl.contains( " -Duser.dir=" ) );
	}
}
