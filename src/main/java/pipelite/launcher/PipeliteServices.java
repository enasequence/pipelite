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

import org.springframework.util.Assert;
import pipelite.lock.PipeliteLocker;
import pipelite.service.*;

public class PipeliteServices {

  private final ScheduleService schedule;
  private final ProcessService process;
  private final StageService stage;
  private final MailService mail;
  private final PipeliteLocker locker;
  private final RegisteredPipelineService registeredPipeline;
  private final InternalErrorService internalError;

  public PipeliteServices(
      ScheduleService schedule,
      ProcessService process,
      StageService stage,
      MailService mail,
      PipeliteLocker locker,
      RegisteredPipelineService registeredPipeline,
      InternalErrorService internalError) {
    Assert.notNull(schedule, "Missing schedule service");
    Assert.notNull(process, "Missing process service");
    Assert.notNull(stage, "Missing stage service");
    Assert.notNull(mail, "Missing mail service");
    Assert.notNull(locker, "Missing locker service");
    Assert.notNull(registeredPipeline, "Missing registered pipeline service");
    Assert.notNull(internalError, "Missing internal error service");
    this.schedule = schedule;
    this.process = process;
    this.stage = stage;
    this.mail = mail;
    this.locker = locker;
    this.registeredPipeline = registeredPipeline;
    this.internalError = internalError;
  }

  public PipeliteServices(
      ScheduleService schedule,
      ProcessService process,
      StageService stage,
      MailService mail,
      PipeliteLockerService locker,
      RegisteredPipelineService registeredPipeline,
      InternalErrorService internalError) {
    this(
        schedule,
        process,
        stage,
        mail,
        locker.getPipeliteLocker(),
        registeredPipeline,
        internalError);
  }

  public ScheduleService schedule() {
    return schedule;
  }

  public ProcessService process() {
    return process;
  }

  public StageService stage() {
    return stage;
  }

  public MailService mail() {
    return mail;
  }

  public PipeliteLocker locker() {
    return locker;
  }

  public RegisteredPipelineService registeredPipeline() {
    return registeredPipeline;
  }

  public InternalErrorService internalError() {
    return internalError;
  }
}
