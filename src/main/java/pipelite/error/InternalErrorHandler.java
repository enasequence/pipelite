/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.error;

import java.util.function.Consumer;
import lombok.extern.flogger.Flogger;
import pipelite.service.InternalErrorService;

/**
 * Executes an action and suppresses exceptions. Saves and logs any exceptions as internal errors.
 * Executes an optional recovery action.
 */
@Flogger
public class InternalErrorHandler {

  private final InternalErrorService internalErrorService;

  private final String pipelineName;
  private final String processId;
  private final String stageName;
  private final Object caller;

  public InternalErrorHandler(
      InternalErrorService internalErrorService,
      String pipelineName,
      String processId,
      String stageName,
      Object caller) {
    this.internalErrorService = internalErrorService;
    this.pipelineName = pipelineName;
    this.processId = processId;
    this.stageName = stageName;
    this.caller = caller;
  }

  public InternalErrorHandler(
      InternalErrorService internalErrorService,
      String pipelineName,
      String processId,
      Object caller) {
    this.internalErrorService = internalErrorService;
    this.pipelineName = pipelineName;
    this.processId = processId;
    this.stageName = null;
    this.caller = caller;
  }

  public InternalErrorHandler(
      InternalErrorService internalErrorService, String pipelineName, Object caller) {
    this.internalErrorService = internalErrorService;
    this.pipelineName = pipelineName;
    this.processId = null;
    this.stageName = null;
    this.caller = caller;
  }

  public InternalErrorHandler(InternalErrorService internalErrorService, Object caller) {
    this.internalErrorService = internalErrorService;
    this.pipelineName = null;
    this.processId = null;
    this.stageName = null;
    this.caller = caller;
  }

  public interface Action {
    void apply();
  }

  /**
   * Executes the action and saves and logs any exceptions.
   *
   * @param action the action to execute
   * @return true if no exception was thrown
   */
  public boolean execute(Action action) {
    return execute(action, null);
  }

  /**
   * Executes the action and saves and logs any exceptions. Executes the recovery action if there
   * were any exceptions.
   *
   * @param action the action to execute
   * @param recover the recovery action
   * @return true if no exception was thrown
   */
  public boolean execute(Action action, Consumer<Exception> recover) {
    try {
      action.apply();
      return true;
    } catch (Exception ex) {
      try {
        internalErrorService.saveInternalError(
            pipelineName, processId, stageName, caller.getClass(), ex);
      } catch (Exception ex2) {
        // Do nothing
      }
      try {
        if (recover != null) {
          recover.accept(ex);
        }
      } catch (Exception ex2) {
        // Do nothing
      }
    }
    return false;
  }
}
