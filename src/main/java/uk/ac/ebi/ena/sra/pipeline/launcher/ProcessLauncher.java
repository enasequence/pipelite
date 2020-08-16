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
import pipelite.process.instance.ProcessInstance;
import pipelite.task.executor.TaskExecutor;
import pipelite.task.instance.LatestTaskExecution;
import pipelite.task.instance.TaskInstance;
import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;
import pipelite.process.state.ProcessExecutionState;
import pipelite.task.state.TaskExecutionState;
import pipelite.task.result.TaskExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;
import pipelite.lock.ProcessInstanceLocker;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleStorage;
import uk.ac.ebi.ena.sra.pipeline.storage.ProcessLogBean;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;

public class ProcessLauncher implements PipeliteProcess {

  private final String launcherName;
  private final TaskExecutionResultExceptionResolver resolver;
  private final ProcessInstanceLocker locker;

  private static final String MAIL_APPENDER = "MAIL_APPENDER";
  private final Logger log;
  private String process_id;
  private String pipeline_name;
  private ProcessInstance processInstance;
  private TaskInstance[] instances;
  private StorageBackend storage;
  private TaskExecutor executor;
  private Stage[] stages;
  private String __name;
  private int max_redo_count = 1;
  private volatile boolean do_stop;

  public ProcessLauncher(String launcherName, TaskExecutionResultExceptionResolver resolver, ProcessInstanceLocker locker) {
    this.launcherName = launcherName;
    this.resolver = resolver;
    this.locker = locker;

    PatternLayout layout = createLayout();
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
  public ProcessInstance getProcessInstance() {
    return processInstance;
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
        if (!lockProcessInstance()) {
          log.error(String.format("There were problems while locking process %s.", getProcessId()));
          return;
        }

        load_state();
        save_state(); // this is to check permissions

        if (ProcessExecutionState.ACTIVE != processInstance.getState()) {
          log.warn(
              String.format(
                  "Invoked for process %s with state %s.", getProcessId(), processInstance.getState()));
          processInstance.setState(ProcessExecutionState.ACTIVE);
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
          log.warn(String.format("Terminal state reached for %s", processInstance));
        } else {
          increment_process_counter();
          execute_stages();
          save_stages();
          if (eval_process()) {
            if (0 < processInstance.getExecutionCount() && 0 == processInstance.getExecutionCount() % max_redo_count)
              processInstance.setState(ProcessExecutionState.FAILED);
          }
        }
        save_state();
      } catch (StorageException e) {
        log.error(e.getMessage(), e);

      } finally {
        //            unlock_stages();
        unlockProcessInstance();
        purge_stages();
      }
    }
  }

  private void purge_stages() {
    instances = null;
  }

  private void increment_process_counter() {
    processInstance.incrementExecutionCount();
  }

  private boolean lockProcessInstance() {
    return locker.lock(launcherName, processInstance);
  }

  private void unlockProcessInstance() {
    if (locker.isLocked(processInstance)) {
      locker.unlock(launcherName, processInstance);
    }
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

    for (TaskInstance instance : instances) {
      log.info(
              String.format(
                      "Stage [%s], enabled [%b] result [%s] of type [%s], count [%d]",
                      instance.getTaskName(),
                      instance.isEnabled(),
                      instance.getLatestTaskExecution().getResultName(),
                      executor.getTaskExecutionState(instance),
                      instance.getExecutionCount()));
      switch (executor.getTaskExecutionState(instance)) {
        case ACTIVE:
          break;

        case DISABLED:
          to_process--;
          break;

        case COMPLETED:
          // to_process -= to_process;
          LatestTaskExecution ei = instance.getLatestTaskExecution();
          processInstance.setState(
                  null != ei && ei.getResultType().isError() ? ProcessExecutionState.FAILED : ProcessExecutionState.COMPLETED);
          return false;
      }
    }

    // no stages to process
    if (0 >= to_process) {
      processInstance.setState(ProcessExecutionState.COMPLETED);
      return false;
    }
    return true;
  }

  private void init_state() {
    processInstance = new ProcessInstance();
    processInstance.setPipelineName(pipeline_name);
    processInstance.setProcessId(process_id);
  }

  private void load_state() {
    try {
      storage.load(processInstance);
    } catch (StorageException e) {
      log.error(e.getMessage(), e);
    }
  }

  private void save_state() {
    try {
      storage.save(processInstance);
    } catch (StorageException e) {
      log.error(e.getMessage(), e);
    }
  }

  private void init_stages() {
    Stage[] stages = getStages();
    instances = new TaskInstance[stages.length];

    for (int i = 0; i < instances.length; ++i) {
      Stage stage = stages[i];
      TaskInstance instance = new TaskInstance();
      instance.setTaskExecutorConfig(stage.getExecutorConfig());
      instance.setTaskName(stage.toString());
      instance.setProcessId(process_id);
      instance.setProcessName(pipeline_name);
      instance.setDependsOn(null == stage.getDependsOn() ? null : stage.getDependsOn().toString());
      instance.setMemory(stage.getMemoryLimit());
      instance.setCores(stage.getCPUCores());
      instance.setPropertiesPass(stage.getPropertiesPass());

      instances[i] = instance;
    }
  }

  private boolean load_stages() {
    boolean result = true;
    for (TaskInstance instance : instances) {
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
        bean.setStage(instance.getTaskName());
        bean.setThrowable(se);
        bean.setMessage(bean_message);
        bean.setLSFJobID(null);
        bean.setLSFHosts(null);
        bean.setExecutionId(
            (null == instance.getLatestTaskExecution()
                ? null
                : instance.getLatestTaskExecution().getExecutionId()));
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
    for (TaskInstance instance : instances) storage.save(instance);
  }

  private void execute_stages() throws StorageException {
    for (TaskInstance instance :
        instances) // TODO: replace with eval.next() and whole process re-evaluation
    {
      if (do_stop) break;

      if (TaskExecutionState.ACTIVE == executor.getTaskExecutionState(instance)) {

        LatestTaskExecution ei = instance.getLatestTaskExecution();
        ei.setStartTime(new Timestamp(System.currentTimeMillis()));
        // todo set id
        ei.setExecutionId(storage.getExecutionId());
        storage.save(instance);

        executor.execute(instance);

        ei.setEndTime(new Timestamp(System.currentTimeMillis()));
        ExecutionInfo info = executor.get_info();

        instance.setExecutionCount(instance.getExecutionCount() + 1);
        storage.save(instance);

        List<TaskInstance> dependend = invalidate_dependands(instance);
        for (TaskInstance si : dependend) storage.save(si);

        // Translate execution result to exec status
        TaskExecutionResult result;
        if (null != info.getThrowable()) {
          result = resolver.resolveError(info.getThrowable());
        } else {
          result = resolver.exitCodeSerializer().deserialize(info.getExitCode());
        }

        ei.setTaskExecutionResult(result);
        ei.setStderr(info.getStderr());
        ei.setStdout(info.getStdout());
        ei.setCmd(info.getCommandline());

        storage.save(ei);
        storage.flush();

        if (result.isError()) {
          emit_log(instance, info);
          break;
        }
      }
    }
  }

  private void emit_log(TaskInstance instance, ExecutionInfo info) {
    ProcessLogBean bean = new ProcessLogBean();
    // TODO: eval usage of Throwable, ExceptionText and Message
    bean.setThrowable(info.getThrowable());
    bean.setExceptionText(info.getLogMessage());
    bean.setMessage(instance.getLatestTaskExecution().getResultName());
    bean.setLSFHosts(info.getHost());
    bean.setLSFJobID(null != info.getPID() ? info.getPID().longValue() : null);
    bean.setProcessID(instance.getProcessId());
    bean.setStage(instance.getTaskName());
    bean.setPipelineName(instance.getProcessName());
    bean.setExecutionId(
        (null == instance.getLatestTaskExecution()
            ? null
            : instance.getLatestTaskExecution().getExecutionId()));
    try {
      storage.save(bean);
    } catch (StorageException e) {
      log.error(e.getMessage(), e);
    }
  }

  private List<TaskInstance> invalidate_dependands(TaskInstance from_instance) {
    List<TaskInstance> result = new ArrayList<>(getStages().length);
    invalidate_dependands(from_instance, false, result);
    return result;
  }

  private void invalidate_dependands(
          TaskInstance from_instance, boolean reset, List<TaskInstance> touched) {
    for (TaskInstance i : instances) {
      if (i.equals(from_instance)) continue;

      if (null == i.getDependsOn()) continue;

      if (i.getDependsOn().equals(from_instance.getTaskName()))
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

  @Override
  public void setProcessID(String process_id) {
    this.process_id = process_id;
  }

  @Override
  public String getProcessId() {
    return process_id;
  }

  @Override
  public TaskExecutor getExecutor() {
    return this.executor;
  }

  @Override
  public ProcessInstanceLocker getLocker() {
    return locker;
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
