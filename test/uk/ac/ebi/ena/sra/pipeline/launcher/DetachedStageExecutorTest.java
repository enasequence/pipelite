package uk.ac.ebi.ena.sra.pipeline.launcher;

import org.junit.Assert;
import org.junit.Test;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;

public class DetachedStageExecutorTest {
  @Test public void
  javaMemory()
  {
    DetachedStageExecutor se = new DetachedStageExecutor( "TEST",
        new ResultTranslator( new ExecutionResult[] { new ExecutionResult() {
          @Override public RESULT_TYPE getType() { return null; }
          @Override public byte getExitCode() { return 0; }
          @Override public Class<? extends Throwable> getCause() { return null; }
          @Override public String getMessage() { return null; } } } ),
        "NOFILE", "NOPATH",  new String[] { } );

    se.execute( new StageInstance()
    {
      {
        setEnabled( true );
        setPropertiesPass( new String[] { } );
        setMemoryLimit( 2000 );
      }
    } );

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue( cmdl.contains( " -Xmx2000M" ) );
  }


  @Test public void
  javaMemoryNotSet()
  {
    DetachedStageExecutor se = new DetachedStageExecutor( "TEST",
        new ResultTranslator( new ExecutionResult[] { new ExecutionResult() {
          @Override public RESULT_TYPE getType() { return null; }
          @Override public byte getExitCode() { return 0; }
          @Override public Class<? extends Throwable> getCause() { return null; }
          @Override public String getMessage() { return null; } } } ),
        "NOFILE", "NOPATH",  new String[] { } );

    se.execute( new StageInstance()
    {
      {
        setEnabled( true );
        setPropertiesPass( new String[] { } );
      }
    } );

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue( !cmdl.contains( " -Xmx2000M" ) );
  }


  @Test public void
  prefixAndSource()
  {
    String prefix = "NOFILE";
    String source = "NOPATH";
    DetachedStageExecutor se = new DetachedStageExecutor( "TEST",
        new ResultTranslator( new ExecutionResult[] { new ExecutionResult() {
          @Override public RESULT_TYPE getType() { return null; }
          @Override public byte getExitCode() { return 0; }
          @Override public Class<? extends Throwable> getCause() { return null; }
          @Override public String getMessage() { return null; } } } ),
        prefix, source, new String[] { } );

    se.execute( new StageInstance()
    {
      {
        setEnabled( true );
        setPropertiesPass( new String[] { } );
      }
    } );

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue( cmdl.contains( " -D" +  prefix + "=" + source ) );
  }


  @Test public void
  propertiesPassStageSpecific()
  {
    String prefix = "NOFILE";
    String source = "NOPATH";
    DetachedStageExecutor se = new DetachedStageExecutor( "TEST",
        new ResultTranslator( new ExecutionResult[] { new ExecutionResult() {
          @Override public RESULT_TYPE getType() { return null; }
          @Override public byte getExitCode() { return 0; }
          @Override public Class<? extends Throwable> getCause() { return null; }
          @Override public String getMessage() { return null; } } } ),
        prefix, source, new String[] { "user.dir" } );

    se.configure( null );

    se.execute( new StageInstance()
    {
      {
        setEnabled( true );
        setPropertiesPass( new String[] { } );
        setPropertiesPass( new String [] { "user.country" } );
      }
    } );

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue( cmdl.contains( " -Duser.country=" ) );
    Assert.assertTrue( cmdl.contains( " -Duser.dir=" ) );
  }
}
