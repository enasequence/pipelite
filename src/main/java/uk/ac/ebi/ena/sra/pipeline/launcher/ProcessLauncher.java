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
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.net.SMTPAppender;
import pipelite.task.executor.AbstractTaskExecutor;
import pipelite.task.executor.TaskExecutor;
import pipelite.task.result.TaskExecutionResultTranslator;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState.State;
import pipelite.task.state.TaskExecutionState;
import pipelite.task.result.TaskExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;
import uk.ac.ebi.ena.sra.pipeline.resource.ProcessResourceLock;
import uk.ac.ebi.ena.sra.pipeline.resource.ResourceLocker;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleStorage;
import uk.ac.ebi.ena.sra.pipeline.storage.ProcessLogBean;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;

public class ProcessLauncher implements PipeliteProcess {
  private static final String MAIL_APPENDER = "MAIL_APPENDER";
  private Logger log;
  private String process_id;
  private String pipeline_name;
  private final PatternLayout layout;
  PipeliteState state;
  private StageInstance[] instances;
  private StorageBackend storage;
  private TaskExecutor executor;
  private TaskExecutionResultTranslator translator;
  private Stage[] stages;
  private ResourceLocker locker;
  private TaskExecutionResult[] commit_statuses;
  private String __name;
  private int max_redo_count = 1;
  private volatile boolean do_stop;

  public ProcessLauncher() {
    layout = createLayout();
    log = Logger.getLogger(process_id + " " + getClass().getSimpleName());
    log.removeAllAppenders();
    log.addAppender(new ConsoleAppender(layout));
  }

  private static PatternLayout createLayout() {
    return new PatternLayout("%d{ISO8601} %-5p [%t] %c{1}:%L - %m%n");
  }

  @Override
  public void setExecutor(TaskExecutor executor) {
    this.executor = executor;
  }

  @Override
  public void setStorage(StorageBackend storage) {
    this.storage = storage;
  }

  @Override
  public StorageBackend getStorage() {
    return this.storage;
  }

  public void setStages(Stage[] stages) {
    this.stages = stages;
  }

  public Stage[] getStages() {
    return stages;
  }

  @Override
  public void run() {
    decorateThreadName();
    lifecycle();
    undecorateThreadName();
  }

  public void decorateThreadName() {
    __name = Thread.currentThread().getName();
    Thread.currentThread()
        .setName(Thread.currentThread().getName() + "@" + getPipelineName() + "/" + getProcessId());
  }

  public void undecorateThreadName() {
    Thread.currentThread().setName(__name);
  }

  /* TODO: possible split to extract initialisation */
  void lifecycle() {
    if (!do_stop) {
      try {
        init_state();
        init_stages();

        load_state();
        if (!lock_process()) {
          log.error(String.format("There were problems while locking process %s.", getProcessId()));
          return;
        }

        load_state();
        save_state(); // this is to check permissions

        if (State.ACTIVE != state.getState()) {
          log.warn(
              String.format(
                  "Invoked for process %s with state %s.", getProcessId(), state.getState()));
          state.setState(State.ACTIVE);
        }

        if (!load_stages()) {
          log.error(
              String.format(
                  "There were problems while loading stages for process %s.", getProcessId()));
          return;
        }

        //            if( !lock_stages() )
        //            {
        //                log.error( String.format( "There were problems while locking process or
        // stages for process %s.", getProcessId() ) );
        //                return;
        //            }

        if (!load_stages()) {
          log.error(
              String.format(
                  "There were problems while loading stages for process %s.", getProcessId()));
          return;
        }

        save_stages(); // this is to check database permissions

        if (!eval_process()) {
          log.warn(String.format("Terminal state reached for %s", state));
        } else {
          increment_process_counter();
          execute_stages();
          save_stages();
          if (eval_process()) {
            if (0 < state.getExecCount() && 0 == state.getExecCount() % max_redo_count)
              state.setState(State.FAILED);
          }
        }
        save_state();
      } catch (StorageException e) {
        log.error(e.getMessage(), e);

      } finally {
        //            unlock_stages();
        unlock_process();
        purge_stages();
      }
    }
  }

  private void purge_stages() {
    instances = null;
  }

  private void increment_process_counter() {
    state.exec_cnt++;
  }

  private boolean lock_process() {
    return locker.lock(new ProcessResourceLock(state.getPipelineName(), state.getProcessId()));
  }

  private void unlock_process() {
    if (locker.is_locked(new ProcessResourceLock(state.getPipelineName(), state.getProcessId())))
      locker.unlock(new ProcessResourceLock(state.getPipelineName(), state.getProcessId()));
    ;
  }

  // Existing statuses:
  // 1 unknown /not processed.  StageTransient
  // 2 permanent success.       StageTerminal
  // 3 transient success.       StageTransient
  // 4 permanent failure.       ProcessTerminal
  // 5 transient failure.       StageTransient
  // 6 >ExecutionCounter.       ProcessTerminal

  private boolean eval_process() {
    int to_process = instances.length;

    loop:
    for (int i = 0; i < instances.length; ++i) {
      StageInstance instance = instances[i];
      log.info(
          String.format(
              "Stage [%s], enabled [%b] result [%s] of type [%s], count [%d]",
              instance.getStageName(),
              instance.isEnabled(),
              instance.getExecutionInstance().getResult(),
              executor.can_execute(instance),
              instance.getExecutionCount()));
      switch (executor.can_execute(instance)) {
        case ACTIVE_TASK:
          break;

        case DISABLED_TASK:
          to_process--;
          break;

        case COMPLETED_TASK:
          // to_process -= to_process;
          ExecutionInstance ei = instance.getExecutionInstance();
          state.setState(
              null != ei && ei.getResultType().isError() ? State.FAILED : State.COMPLETED);
          return false;
      }
    }

    // no stages to process
    if (0 >= to_process) {
      state.setState(State.COMPLETED);
      return false;
    }
    return true;
  }

  private void init_state() {
    state = new PipeliteState();
    state.setPipelineName(pipeline_name);
    state.setProcessId(process_id);
  }

  private void load_state() {
    try {
      storage.load(state);
    } catch (StorageException e) {
      log.error(e.getMessage(), e);
    }
  }

  private void save_state() {
    try {
      storage.save(state);
    } catch (StorageException e) {
      log.error(e.getMessage(), e);
    }
  }

  private void init_stages() {
    Stage[] stages = getStages();
    instances = new StageInstance[stages.length];
    translator = new TaskExecutionResultTranslator(commit_statuses);

    for (int i = 0; i < instances.length; ++i) {
      Stage stage = stages[i];
      StageInstance instance = new StageInstance();
      instance.setResourceConfig(stage.getExecutorConfig());
      instance.setStageName(stage.toString());
      instance.setProcessID(process_id);
      instance.setPipelineName(pipeline_name);
      instance.setDependsOn(null == stage.getDependsOn() ? null : stage.getDependsOn().toString());
      instance.setMemoryLimit(stage.getMemoryLimit());
      instance.setCPUCores(stage.getCPUCores());
      instance.setPropertiesPass(stage.getPropertiesPass());

      instances[i] = instance;
    }
  }

  private boolean load_stages() {
    boolean result = true;
    for (StageInstance instance : instances) {
      try {
        storage.load(instance);
      } catch (StorageException se) {
        result = false;
        Throwable t = se.getCause();
        String bean_message = "Unable to load stage";
        if (t instanceof SQLException && 54 == ((SQLException) t).getErrorCode()) {
          // LOCKED: code is 54 //state 61000
          log.info(((SQLException) t).getSQLState());
          bean_message = "Unable to lock process";
        }

        ProcessLogBean bean = new ProcessLogBean();
        bean.setPipelineName(getPipelineName());
        bean.setProcessID(getProcessId());
        bean.setStage(instance.getStageName());
        bean.setThrowable(se);
        bean.setMessage(bean_message);
        bean.setLSFJobID(null);
        bean.setLSFHosts(null);
        bean.setExecutionId(
            (null == instance.getExecutionInstance()
                ? null
                : instance.getExecutionInstance().getExecutionId()));
        try {
          storage.save(bean);
        } catch (StorageException se1) {
          log.error(se1.getMessage(), se1);
        }
      }
    }

    return result;
  }

  private void save_stages() throws StorageException {
    for (StageInstance instance : instances) storage.save(instance);
  }

  private void execute_stages() throws StorageException {
    for (StageInstance instance :
        instances) // TODO: replace with eval.next() and whole process re-evaluation
    {
      if (do_stop) break;

      if (TaskExecutionState.ACTIVE_TASK == executor.can_execute(instance)) {
        if (null != instance.getResourceConfig(executor.getConfigClass()))
          executor.configure(instance.getResourceConfig(executor.getConfigClass()));

        ExecutionInstance ei = instance.getExecutionInstance();
        ei.setStartTime(new Timestamp(System.currentTimeMillis()));
        // todo set id
        ei.setExceutionId(storage.getExecutionId());
        storage.save(instance);

        executor.execute(instance);

        ei.setFinishTime(new Timestamp(System.currentTimeMillis()));
        ExecutionInfo info = executor.get_info();

        instance.setExecutionCount(instance.getExecutionCount() + 1);
        storage.save(instance);

        List<StageInstance> dependend = invalidate_dependands(instance);
        for (StageInstance si : dependend) storage.save(si);

        // Translate execution result to exec status
        TaskExecutionResult result = null;
        if (null != info.getThrowable()) {
          result = translator.getCommitStatus(info.getThrowable());
        } else {
          result = translator.getCommitStatus(info.getExitCode());
        }

        ei.setResultType(result.getExecutionResultType());
        ei.setResult(result.getExecutionResult());
        ei.setStderr(info.getStderr());
        ei.setStdout(info.getStdout());
        ei.setCmdLine(info.getCommandline());

        storage.save(ei);
        storage.flush();

        if (result.isError()) {
          emit_log(instance, info);
          break;
        }
      }
    }
  }

  private void emit_log(StageInstance instance, ExecutionInfo info) {
    ProcessLogBean bean = new ProcessLogBean();
    // TODO: eval usage of Throwable, ExceptionText and Message
    bean.setThrowable(info.getThrowable());
    bean.setExceptionText(info.getLogMessage());
    bean.setMessage(instance.getExecutionInstance().getResult());
    bean.setLSFHosts(info.getHost());
    bean.setLSFJobID(null != info.getPID() ? info.getPID().longValue() : null);
    bean.setProcessID(instance.getProcessID());
    bean.setStage(instance.getStageName());
    bean.setPipelineName(instance.getPipelineName());
    bean.setExecutionId(
        (null == instance.getExecutionInstance()
            ? null
            : instance.getExecutionInstance().getExecutionId()));
    try {
      storage.save(bean);
    } catch (StorageException e) {
      log.error(e.getMessage(), e);
    }
  }

  private List<StageInstance> invalidate_dependands(StageInstance from_instance) {
    List<StageInstance> result = new ArrayList<>(getStages().length);
    invalidate_dependands(from_instance, false, result);
    return result;
  }

  private void invalidate_dependands(
      StageInstance from_instance, boolean reset, List<StageInstance> touched) {
    for (StageInstance i : instances) {
      if (i.equals(from_instance)) continue;

      if (null == i.getDependsOn()) continue;

      if (i.getDependsOn().equals(from_instance.getStageName()))
        invalidate_dependands(i, true, touched);
    }

    if (reset) {
      executor.reset(from_instance);
      touched.add(from_instance);
    }
  }

  protected static Appender createMailAppender(
      String subj, String smtp_host, String from_address, String send_to, PatternLayout layout) {

    SMTPAppender mailer = new SMTPAppender();
    mailer.setBufferSize(1);
    mailer.setLayout(layout);
    mailer.setTo(send_to);
    mailer.setFrom(from_address);
    mailer.setSubject(subj);
    mailer.setSMTPHost(smtp_host);
    mailer.setThreshold(Level.ERROR);
    mailer.activateOptions();
    mailer.setName(MAIL_APPENDER);
    return mailer;
  }

  public static void main(String args[])
      throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException,
          IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
          SecurityException {
    PatternLayout layout = createLayout();
    ConsoleAppender appender = new ConsoleAppender(layout, "System.out");
    appender.setThreshold(Level.ALL);
    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(appender);
    Logger.getRootLogger().setLevel(Level.ALL);

    Parameters params = new Parameters();
    JCommander jc = new JCommander(params);
    try {
      jc.parse(args);
    } catch (Exception e) {
      jc.usage();
      System.exit(1);
    }

    run_list(layout, params);
  }

  private static OracleStorage initStorageBackend() {
    OracleStorage os = new OracleStorage();
    os.setProcessTableName(DefaultConfiguration.currentSet().getProcessTableName());
    os.setStageTableName(DefaultConfiguration.currentSet().getStageTableName());
    os.setPipelineName(DefaultConfiguration.currentSet().getPipelineName());
    os.setLogTableName(DefaultConfiguration.currentSet().getLogTableName());
    return os;
  }

  private static void run_list(PatternLayout layout, Parameters params)
      throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException,
          IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
          SecurityException {
    Connection connection = null;

    Stage stage =
        (null == params.stage) ? null : DefaultConfiguration.currentSet().getStage(params.stage);

    try {
      connection = DefaultConfiguration.currentSet().createConnection();

      for (String process_id : params.IDs) {
        Appender a = Logger.getRootLogger().getAppender(MAIL_APPENDER);
        if (null != a) Logger.getRootLogger().removeAppender(a);

        if (null != params.mail_to)
          Logger.getRootLogger()
              .addAppender(
                  createMailAppender(
                      ProcessLauncher.class.getSimpleName() + " failure report: " + process_id,
                      DefaultConfiguration.currentSet().getSMTPServer(),
                      DefaultConfiguration.currentSet().getSMTPMailFrom(),
                      params.mail_to,
                      layout));

        ProcessLauncher process = new ProcessLauncher();
        process.setProcessID(process_id);
        process.setStages(DefaultConfiguration.currentSet().getStages());
        OracleStorage os = initStorageBackend();
        os.setConnection(connection);
        process.setStorage(os);
        process.setLocker(os);
        AbstractTaskExecutor executor =
            (AbstractTaskExecutor)
                (Class.forName(params.executor_class)
                    .getConstructor(String.class, TaskExecutionResultTranslator.class)
                    .newInstance(
                        "",
                        new TaskExecutionResultTranslator(DefaultConfiguration.currentSet().getCommitStatus())));

        process.setExecutor(executor);
        process.lifecycle();
      }
    } finally {
      if (null != connection) {
        try {
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void setCommitStatuses(TaskExecutionResult[] commit_statuses) {
    this.commit_statuses = commit_statuses;
  }

  public TaskExecutionResult[] getCommitStatuses() {
    return commit_statuses;
  }

  @Override
  public void setProcessID(String process_id) {
    this.process_id = process_id;
  }

  @Override
  public String getProcessId() {
    return process_id;
  }

  static class Parameters {
    @Parameter(names = "--executor", description = "Executor class")
    String executor_class = DetachedStageExecutor.class.getName();

    @Parameter(required = true)
    List<String> IDs;

    @Parameter(names = "--stage", description = "Stage name to execute")
    String stage;

    @Parameter(names = "--mail-to", description = "")
    String mail_to = DefaultConfiguration.currentSet().getDefaultMailTo();
  }

  @Override
  public TaskExecutor getExecutor() {
    return this.executor;
  }

  @Override
  public ResourceLocker getLocker() {
    return locker;
  }

  @Override
  public void setLocker(ResourceLocker locker) {
    this.locker = locker;
  }

  public String getPipelineName() {
    return pipeline_name;
  }

  public void setPipelineName(String pipeline_name) {
    this.pipeline_name = pipeline_name;
  }

  public void setRedoCount(int max_redo_count) {
    this.max_redo_count = max_redo_count;
  }

  @Override
  public void stop() {
    this.do_stop = true;
  }

  @Override
  public boolean isStopped() {
    return this.do_stop;
  }
}