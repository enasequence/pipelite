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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Verify;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.entity.PipeliteStageId;
import pipelite.repository.PipeliteProcessRepository;
import pipelite.repository.PipeliteStageRepository;
import pipelite.task.executor.TaskExecutor;
import pipelite.task.instance.TaskInstance;
import pipelite.resolver.ExceptionResolver;
import pipelite.task.result.TaskExecutionResultType;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessLauncherInterface;
import pipelite.process.state.ProcessExecutionState;
import pipelite.task.state.TaskExecutionState;
import pipelite.task.result.TaskExecutionResult;
import pipelite.stage.Stage;
import pipelite.lock.ProcessInstanceLocker;
import uk.ac.ebi.ena.sra.pipeline.storage.ProcessLogBean;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;

@Slf4j
public class ProcessLauncher implements ProcessLauncherInterface {

  private final String launcherName;

  private final PipeliteProcess pipeliteProcess;
  private final ExceptionResolver resolver;
  private final ProcessInstanceLocker locker;
  private final PipeliteProcessRepository pipeliteProcessRepository;
  private final PipeliteStageRepository pipeliteStageRepository;

  private TaskInstance[] instances;
  private StorageBackend storage;
  private TaskExecutor executor;
  private Stage[] stages;
  private String __name;
  private int max_redo_count = 1;
  private volatile boolean do_stop;

  public ProcessLauncher(
      String launcherName,
      PipeliteProcess pipeliteProcess,
      ExceptionResolver resolver,
      ProcessInstanceLocker locker,
      @Autowired PipeliteProcessRepository pipeliteProcessRepository,
      @Autowired PipeliteStageRepository pipeliteStageRepository) {

    Verify.verifyNotNull(launcherName);
    Verify.verifyNotNull(pipeliteProcess);

    this.launcherName = launcherName;
    this.pipeliteProcess = pipeliteProcess;
    this.resolver = resolver;
    this.locker = locker;
    this.pipeliteProcessRepository = pipeliteProcessRepository;
    this.pipeliteStageRepository = pipeliteStageRepository;
  }

  @Override
  public void setExecutor(TaskExecutor executor) {
    this.executor = executor;
  }

  @Override
  public PipeliteProcess getPipeliteProcess() {
    return pipeliteProcess;
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
        init_stages();

        if (!lockProcessInstance()) {
          log.error(String.format("There were problems while locking process %s.", getProcessId()));
          return;
        }

        if (ProcessExecutionState.ACTIVE != pipeliteProcess.getState()) {
          log.warn(
              String.format(
                  "Invoked for process %s with state %s.",
                  getProcessId(), pipeliteProcess.getState()));
          pipeliteProcess.setState(ProcessExecutionState.ACTIVE);
        }

        if (!load_stages()) {
          log.error(
              String.format(
                  "There were problems while loading stages for process %s.", getProcessId()));
          return;
        }

        // save_stages(); // this is to check database permissions

        if (!eval_process()) {
          log.warn(String.format("Terminal state reached for %s", pipeliteProcess));
        } else {
          increment_process_counter();
          execute_stages();
          save_stages();
          if (eval_process()) {
            if (0 < pipeliteProcess.getExecutionCount()
                && 0 == pipeliteProcess.getExecutionCount() % max_redo_count)
              pipeliteProcess.setState(ProcessExecutionState.FAILED);
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
    pipeliteProcess.incrementExecutionCount();
  }

  private boolean lockProcessInstance() {
    return locker.lock(launcherName, pipeliteProcess);
  }

  private void unlockProcessInstance() {
    if (locker.isLocked(pipeliteProcess)) {
      locker.unlock(launcherName, pipeliteProcess);
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
              instance.getPipeliteStage().getStageName(),
              instance.getPipeliteStage().getEnabled(),
              instance.getPipeliteStage().getResult(),
              executor.getTaskExecutionState(instance),
              instance.getPipeliteStage().getExecutionCount()));
      switch (executor.getTaskExecutionState(instance)) {
        case ACTIVE:
          break;

        case DISABLED:
          to_process--;
          break;

        case COMPLETED:
          TaskExecutionResultType resultType = instance.getPipeliteStage().getResultType();
          pipeliteProcess.setState(
              null != resultType && resultType.isError()
                  ? ProcessExecutionState.FAILED
                  : ProcessExecutionState.COMPLETED);
          return false;
      }
    }

    // no stages to process
    if (0 >= to_process) {
      pipeliteProcess.setState(ProcessExecutionState.COMPLETED);
      return false;
    }
    return true;
  }

  private void save_state() {
    pipeliteProcessRepository.save(pipeliteProcess);
  }

  private void init_stages() throws StorageException {
    Stage[] stages = getStages();
    instances = new TaskInstance[stages.length];

    for (int i = 0; i < instances.length; ++i) {
      Stage stage = stages[i];
      TaskInstance instance = new TaskInstance(stage);
      instance.setPipeliteStage(
          PipeliteStage.newExecution(
              pipeliteProcess.getProcessId(),
              pipeliteProcess.getProcessName(),
              stage.toString(),
              storage.getExecutionId()));
      instance.setTaskExecutorConfig(stage.getExecutorConfig());
      instance.setMemory(stage.getMemory());
      instance.setCores(stage.getCores());
      instance.setJavaSystemProperties(stage.getPropertiesPass());
      instances[i] = instance;
    }
  }

  private boolean load_stages() {
    for (TaskInstance instance : instances) {
      Optional<PipeliteStage> pipeliteStageSaved =
          pipeliteStageRepository.findById(
              new PipeliteStageId(
                  instance.getPipeliteStage().getProcessId(),
                  instance.getPipeliteStage().getProcessName(),
                  instance.getPipeliteStage().getStageName()));
      if (!pipeliteStageSaved.isPresent()) {
        ProcessLogBean bean = new ProcessLogBean();
        bean.setPipelineName(getPipelineName());
        bean.setProcessID(getProcessId());
        bean.setStage(instance.getPipeliteStage().getStageName());
        bean.setMessage("Unable to load stage");
        bean.setExecutionId(instance.getPipeliteStage().getExecutionId());
        try {
          storage.save(bean);
        } catch (StorageException se1) {
          log.error(se1.getMessage(), se1);
        }
        return false;
      }
      instance.setPipeliteStage(pipeliteStageSaved.get());
    }
    return true;
  }

  private void save_stages() {
    for (TaskInstance instance : instances) {
      pipeliteStageRepository.save(instance.getPipeliteStage());
    }
  }

  private void execute_stages() throws StorageException {
    for (TaskInstance instance :
        instances) // TODO: replace with eval.next() and whole process re-evaluation
    {
      if (do_stop) break;

      if (TaskExecutionState.ACTIVE == executor.getTaskExecutionState(instance)) {

        instance.getPipeliteStage().retryExecution(storage.getExecutionId());
        pipeliteStageRepository.save(instance.getPipeliteStage());

        executor.execute(instance);

        ExecutionInfo info = executor.get_info();

        // Translate execution result to exec status
        TaskExecutionResult result;
        if (null != info.getThrowable()) {
          result = resolver.resolveError(info.getThrowable());
        } else {
          result = resolver.exitCodeSerializer().deserialize(info.getExitCode());
        }

        instance
            .getPipeliteStage()
            .endExecution(result, info.getCommandline(), info.getStdout(), info.getStderr());
        pipeliteStageRepository.save(instance.getPipeliteStage());

        List<TaskInstance> dependend = invalidate_dependands(instance);
        for (TaskInstance si : dependend) {
          pipeliteStageRepository.save(si.getPipeliteStage());
        }

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
    bean.setThrowable(info.getThrowable());
    bean.setExceptionText(info.getLogMessage());
    bean.setMessage(instance.getPipeliteStage().getResult());
    bean.setLSFHosts(info.getHost());
    bean.setLSFJobID(null != info.getPID() ? info.getPID().longValue() : null);
    bean.setProcessID(instance.getPipeliteStage().getProcessId());
    bean.setStage(instance.getPipeliteStage().getStageName());
    bean.setPipelineName(instance.getPipeliteStage().getProcessName());
    bean.setExecutionId(instance.getPipeliteStage().getExecutionId());
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
    for (TaskInstance taskInstance : instances) {
      if (taskInstance.equals(from_instance)) {
        continue;
      }

      Stage stageDependsOn = taskInstance.getStage().getDependsOn();
      if (stageDependsOn != null
          && stageDependsOn.getStageName().equals(from_instance.getStage().getStageName())) {
        invalidate_dependands(taskInstance, true, touched);
      }
    }

    if (reset) {
      executor.reset(from_instance);
      touched.add(from_instance);
    }
  }

  @Override
  public String getProcessId() {
    return pipeliteProcess.getProcessId();
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
    return pipeliteProcess.getProcessName();
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
