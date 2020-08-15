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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.sql.Connection;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import pipelite.task.executor.TaskExecutor;
import pipelite.task.result.resolver.ExecutionResultExceptionResolver;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;

public class StageLauncher {
  static final PatternLayout layout = new PatternLayout("%d{ISO8601} %-5p [%t] %c{1}:%L - %m%n");
  protected Connection connection;

  public static final String PARAMETERS_NAME_ID = "--id";
  public static final String PARAMETERS_NAME_STAGE = "--stage";
  public static final String PARAMETERS_NAME_FORCE_COMMIT = "--commit";
  public static final String PARAMETERS_NAME_ENABLED = "--enabled";
  public static final String PARAMETERS_NAME_EXEC_COUNT = "--exec-cnt";

  @Parameter(names = PARAMETERS_NAME_ID, description = "ID of the task", required = true)
  String process_id;

  @Parameter(names = PARAMETERS_NAME_STAGE, description = "Stage name to execute", required = true)
  String stage_name;

  @Parameter(names = PARAMETERS_NAME_FORCE_COMMIT, description = "force commit client data")
  boolean force;

  // hidden
  @Parameter(
      names = PARAMETERS_NAME_ENABLED,
      description = "stage is enabled",
      hidden = true,
      arity = 1)
  final
  boolean enabled = true;
  // hidden
  @Parameter(names = PARAMETERS_NAME_EXEC_COUNT, description = "execution counter", hidden = true)
  final
  int exec_cnt = 0;

  public static void main(String[] args) {
    ConsoleAppender appender = new ConsoleAppender(layout, "System.out");
    appender.setThreshold(Level.INFO);

    String os = System.getProperty("os.name");

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(appender);
    Logger.getRootLogger().setLevel(Level.ALL);

    Logger.getRootLogger().warn("Operating system is " + os);

    StageLauncher sl = new StageLauncher();
    JCommander jc = new JCommander(sl);

    try {
      jc.parse(args);

      ExecutionResultExceptionResolver resolver = DefaultConfiguration.CURRENT.getResolver();

      System.exit(sl.execute(resolver));

    } catch (ParameterException pe) {

      jc.usage();
      System.exit(1);
    }
  }

  public int execute(ExecutionResultExceptionResolver resolver) {
    TaskExecutor executor = new InternalStageExecutor(resolver);

    StageInstance instance = new StageInstance();
    instance.setProcessID(process_id);
    instance.setStageName(stage_name);
    instance.setEnabled(enabled);
    instance.setExecutionCount(exec_cnt);
    executor.execute(instance);
    return executor.get_info().getExitCode();
  }
}
