/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.launcher;

import pipelite.configuration.TaskConfigurationEx;
import pipelite.executor.AbstractTaskExecutor;
import pipelite.instance.TaskInstance;
import pipelite.task.Task;
import pipelite.task.TaskInfo;

public class InternalTaskExecutor extends AbstractTaskExecutor {

  public InternalTaskExecutor(TaskConfigurationEx taskConfiguration) {
    super(taskConfiguration);
  }

  public ExecutionInfo execute(TaskInstance taskInstance) {
    Throwable exception = null;

    try {
      String processName = taskInstance.getProcessName();
      String processId = taskInstance.getProcessId();
      String taskName = taskInstance.getTaskName();
      Task task =
          taskInstance.getTaskFactory().createTask(new TaskInfo(processName, processId, taskName));
      task.execute(taskInstance);

    } catch (Throwable e) {
      e.printStackTrace();
      exception = e;
    } finally {
      ExecutionInfo info = new ExecutionInfo();
      info.setThrowable(exception);
      info.setExitCode(resolver.exitCodeSerializer().serialize(resolver.resolveError(exception)));
      return info;
    }
  }

  // TODO: handle call from LSFTaskExecutor and DetachedTaskExecutor

  public static final String PARAMETERS_NAME_ID = "--id";
  public static final String PARAMETERS_NAME_STAGE = "--stage";

  /*


    @Parameter( names = PARAMETERS_NAME_ID, description = "ID of the task", required = true )
    String process_id;

    @Parameter( names = PARAMETERS_NAME_STAGE, description = "Stage name to execute", required = true )
    String stage_name;

    @Parameter( names = PARAMETERS_NAME_FORCE_COMMIT, description = "force commit client data" )
    boolean force;


    //hidden
    @Parameter( names = PARAMETERS_NAME_ENABLED, description = "stage is enabled", hidden=true, arity=1 )
    boolean enabled = true;
    //hidden
    @Parameter( names = PARAMETERS_NAME_EXEC_COUNT, description = "execution counter", hidden=true )
    int    exec_cnt = 0;


    public static void
    main( String[] args ) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
      ConsoleAppender appender = new ConsoleAppender( layout, "System.out" );
      appender.setThreshold( Level.INFO );

      String os = System.getProperty( "os.name" );

      Logger.getRootLogger().removeAllAppenders();
      Logger.getRootLogger().addAppender( appender );
      Logger.getRootLogger().setLevel( Level.ALL );

      Logger.getRootLogger().warn( "Operating system is " + os );

      StageLauncher sl = new StageLauncher();
      JCommander jc = new JCommander( sl );

      try
      {
        jc.parse( args );
        System.exit( sl.execute() );

      } catch( ParameterException pe )
      {

        jc.usage();
        System.exit( 1 );
      }
    }


    public int
    execute()
    {
      StageExecutor executor = new InternalStageExecutor( new ResultTranslator( DefaultConfiguration.currentSet().getCommitStatus() ) )
              .setRedoCount( DefaultConfiguration.currentSet().getStagesRedoCount() );

      StageInstance instance = new StageInstance();
      instance.setProcessID( process_id );
      instance.setTaskName( stage_name );
      instance.setEnabled( enabled );
      instance.setExecutionCount( exec_cnt );
      executor.setClientCanCommit( force );
      executor.execute( instance );
      return executor.get_info().getExitCode();
    }


    public static class
    InternalExecutionResult
    {
      public
      InternalExecutionResult( int exitCode, StageTask task )
      {
        this.exitCode = exitCode;
        this.task = task;
      }


      public final int       exitCode;
      public final StageTask task;
    }


    public InternalExecutionResult
    execute( String process_id, String stage_name, boolean force )
    {
      InternalStageExecutor executor = new InternalStageExecutor( new ResultTranslator( DefaultConfiguration.currentSet().getCommitStatus() ) )
              .setRedoCount( DefaultConfiguration.currentSet().getStagesRedoCount() );
      StageInstance instance = new StageInstance();
      instance.setProcessID( process_id );
      instance.setTaskName( stage_name );
      instance.setEnabled( true );
      instance.setExecutionCount( 0 );
      executor.setClientCanCommit( force );
      executor.execute( instance );
      return new InternalExecutionResult( executor.get_info().getExitCode(), executor.get_task() );
    }
  }

     */
}
