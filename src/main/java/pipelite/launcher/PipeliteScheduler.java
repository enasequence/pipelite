/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.launcher;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.service.*;

@Flogger
public class PipeliteScheduler extends ProcessLauncherService {

  private static final int DEFAULT_MAX_PROCESS_ID_RETRIES = 100;
  private final ProcessFactoryService processFactoryService;
  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final LockService lockService;
  private final Duration processRefreshFrequency;
  private final Map<String, ProcessFactory> processFactoryCache = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncherStats> stats = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> maximumExecutions = new ConcurrentHashMap<>();
  private final List<Schedule> schedules = Collections.synchronizedList(new ArrayList<>());
  private LocalDateTime schedulesValidUntil;

  @Data
  public static class Schedule {
    private ScheduleEntity scheduleEntity;
    private ProcessEntity processEntity;
    private LocalDateTime launchTime;
  }

  public PipeliteScheduler(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      ProcessFactoryService processFactoryService,
      ScheduleService scheduleService,
      ProcessService processService,
      StageService stageService,
      LockService lockService,
      MailService mailService) {
    super(
        launcherConfiguration,
        new PipeliteLocker(lockService, launcherName(launcherConfiguration)),
        (locker1) ->
            new ProcessLauncherPool(
                locker1,
                (pipelineName1, process1) ->
                    new ProcessLauncher(
                        launcherConfiguration,
                        stageConfiguration,
                        processService,
                        stageService,
                        mailService,
                        pipelineName1,
                        process1)));
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(stageConfiguration, "Missing stage configuration");
    Assert.notNull(processFactoryService, "Missing process factory service");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(scheduleService, "Missing schedule service");
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(lockService, "Missing lock service");
    Assert.notNull(mailService, "Missing mail service");
    this.processFactoryService = processFactoryService;
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.lockService = lockService;
    this.processRefreshFrequency = launcherConfiguration.getProcessRefreshFrequency();
  }

  private static String launcherName(LauncherConfiguration launcherConfiguration) {
    return LauncherConfiguration.getSchedulerName(launcherConfiguration);
  }

  @Override
  protected void run() {
    if (schedules.isEmpty() || schedulesValidUntil.isBefore(LocalDateTime.now())) {
      scheduleProcesses();
    }
    for (Schedule schedule : schedules) {
      String pipelineName = schedule.getScheduleEntity().getPipelineName();
      String cronExpression = schedule.getScheduleEntity().getSchedule();
      LocalDateTime launchTime = schedule.getLaunchTime();
      if (!getPool().hasActivePipeline(pipelineName) && launchTime.isBefore(LocalDateTime.now())) {
        if (maximumExecutions.get(pipelineName) != null
            && maximumExecutions.get(pipelineName).decrementAndGet() < 0) {
          continue;
        }
        // Set next launch time.
        schedule.setLaunchTime(CronUtils.launchTime(cronExpression));
        logContext(log.atInfo(), pipelineName)
            .log(
                "Launching %s pipeline with cron expression %s (%s). Next launch time is: %s",
                pipelineName,
                cronExpression,
                schedule.getScheduleEntity().getDescription(),
                launchTime);
        if (createProcessEntity(schedule)) {
          runProcess(schedule);
        }
      }
    }
  }

  @Override
  protected boolean shutdownIfIdle() {
    return !(maximumExecutions.isEmpty()
        || maximumExecutions.values().stream().anyMatch(r -> r.get() > 0));
  }

  private void scheduleProcesses() {
    logContext(log.atInfo()).log("Scheduling processes");
    schedules.clear();
    List<ScheduleEntity> scheduleEntities =
        scheduleService.getAllProcessSchedules(getLauncherName());
    logContext(log.atInfo()).log("Found %s schedules", scheduleEntities.size());
    for (ScheduleEntity scheduleEntity : scheduleEntities) {
      String scheduleDescription = "invalid cron expression";
      if (CronUtils.validate(scheduleEntity.getSchedule())) {
        Schedule schedule = new Schedule();
        schedule.setScheduleEntity(scheduleEntity);
        schedules.add(schedule);

        // TODO: test resume process execution
        boolean isResumeProcessExecution = resumeProcessExecution(schedule);
        if (isResumeProcessExecution) {
          logContext(log.atInfo(), scheduleEntity.getPipelineName())
              .log("Attempting to resume %s pipeline execution", scheduleEntity.getPipelineName());
          runProcess(schedule);
        } else {
          // Update next launch time.
          schedule.setLaunchTime(CronUtils.launchTime(scheduleEntity.getSchedule()));
          scheduleDescription = CronUtils.describe(scheduleEntity.getSchedule());
          logContext(log.atInfo(), scheduleEntity.getPipelineName())
              .log(
                  "Scheduling %s pipeline with cron expression %s (%s). Next launch time is: %s",
                  scheduleEntity.getPipelineName(),
                  scheduleEntity.getSchedule(),
                  scheduleDescription,
                  schedule.getLaunchTime());
        }
      } else {
        logContext(log.atSevere(), scheduleEntity.getPipelineName())
            .log(
                "Ignoring %s pipeline with invalid cron expression",
                scheduleEntity.getPipelineName());
      }
      if (!scheduleDescription.equals(scheduleEntity.getDescription())) {
        scheduleEntity.setDescription(scheduleDescription);
        scheduleService.saveProcessSchedule(scheduleEntity);
      }
    }
    schedulesValidUntil = LocalDateTime.now().plus(processRefreshFrequency);
  }

  private boolean resumeProcessExecution(Schedule schedule) {
    ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
    String pipelineName = scheduleEntity.getPipelineName();
    String processId = scheduleEntity.getProcessId();
    if (scheduleEntity.getStartTime() != null
        && scheduleEntity.getEndTime() == null
        && processId != null) {
      Optional<ProcessEntity> processEntity =
          processService.getSavedProcess(pipelineName, processId);
      if (processEntity.isPresent()) {
        schedule.setProcessEntity(processEntity.get());
        return true;
      } else {
        scheduleEntity.resetExecution();
        scheduleService.saveProcessSchedule(scheduleEntity);
        return false;
      }
    }
    return false;
  }

  public static String getNextProcessId(String processId) {
    if (processId == null) {
      return "1";
    }
    try {
      return String.valueOf(Integer.valueOf(processId) + 1);
    } catch (Exception ex) {
      throw new RuntimeException("Invalid process id " + processId);
    }
  }

  private boolean createProcessEntity(Schedule schedule) {
    String pipelineName = schedule.getScheduleEntity().getPipelineName();
    String processId = getNextProcessId(schedule.getScheduleEntity().getProcessId());
    int remainingProcessIdRetries = DEFAULT_MAX_PROCESS_ID_RETRIES;
    while (true) {
      Optional<ProcessEntity> savedProcessEntity =
          processService.getSavedProcess(pipelineName, processId);
      if (savedProcessEntity.isPresent()) {
        processId = getNextProcessId(savedProcessEntity.get().getProcessId());
      } else {
        break;
      }
      if (--remainingProcessIdRetries <= 0) {
        throw new RuntimeException("Could not assign a new process id");
      }
    }
    ProcessEntity processEntity =
        processService.createExecution(pipelineName, processId, ProcessEntity.DEFAULT_PRIORITY);
    schedule.setProcessEntity(processEntity);
    schedule.getScheduleEntity().startExecution(processId);
    scheduleService.saveProcessSchedule(schedule.getScheduleEntity());
    return true;
  }

  private void runProcess(Schedule schedule) {
    ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
    String pipelineName = scheduleEntity.getPipelineName();
    Process process =
        ProcessFactory.create(
            schedule.getProcessEntity(), cachedProcessFactory(pipelineName), stats(pipelineName));
    if (process != null) {
      getPool()
          .run(
              pipelineName,
              process,
              (p, r) -> {
                scheduleEntity.endExecution();
                scheduleService.saveProcessSchedule(scheduleEntity);
                stats(pipelineName).add(p, r);
              });
    }
  }

  private ProcessFactory cachedProcessFactory(String pipelineName) {
    if (processFactoryCache.containsKey(pipelineName)) {
      return processFactoryCache.get(pipelineName);
    }
    ProcessFactory processFactory = processFactoryService.create(pipelineName);
    processFactoryCache.put(pipelineName, processFactory);
    return processFactory;
  }

  public List<Schedule> getSchedules() {
    return this.schedules;
  }

  private ProcessLauncherStats stats(String pipelineName) {
    stats.putIfAbsent(pipelineName, new ProcessLauncherStats());
    return stats.get(pipelineName);
  }

  public ProcessLauncherStats getStats(String pipelineName) {
    return stats.get(pipelineName);
  }

  public void setMaximumExecutions(String pipelineName, long maximumExecutions) {
    this.maximumExecutions.putIfAbsent(pipelineName, new AtomicLong());
    this.maximumExecutions.get(pipelineName).set(maximumExecutions);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PIPELINE_NAME, pipelineName);
  }
}
