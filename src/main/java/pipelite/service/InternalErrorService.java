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
package pipelite.service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.InternalErrorEntity;
import pipelite.metrics.PipeliteMetrics;
import pipelite.repository.InternalErrorRepository;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Retryable(
    maxAttemptsExpression = "#{@retryService.maxAttempts()}",
    backoff =
        @Backoff(
            delayExpression = "#{@retryService.delay()}",
            maxDelayExpression = "#{@retryService.maxDelay()}",
            multiplierExpression = "#{@retryService.multiplier()}"),
    exceptionExpression = "#{@retryService.recoverableException(#root)}")
@Flogger
public class InternalErrorService {

  private final InternalErrorRepository repository;
  private final PipeliteMetrics metrics;

  public InternalErrorService(
      @Autowired InternalErrorRepository repository, @Autowired PipeliteMetrics metrics) {
    this.repository = repository;
    this.metrics = metrics;
  }

  public InternalErrorEntity saveInternalError(
      String serviceName, String pipelineName, String processId, Class cls, Throwable exception) {
    return saveInternalError(serviceName, pipelineName, processId, null, cls, exception);
  }

  public InternalErrorEntity saveInternalError(
      String serviceName, String pipelineName, Class cls, Throwable exception) {
    return saveInternalError(serviceName, pipelineName, null, null, cls, exception);
  }

  public InternalErrorEntity saveInternalError(String serviceName, Class cls, Throwable exception) {
    return saveInternalError(serviceName, null, null, null, cls, exception);
  }

  public InternalErrorEntity saveInternalError(
      String serviceName,
      String pipelineName,
      String processId,
      String stageName,
      Class cls,
      Throwable exception) {
    try {
      log.atSevere().withCause(exception).log(
          "Internal error in service: %s, pipeline: %s, process: %s, stage: %s, class %s",
          serviceName != null ? serviceName : "",
          pipelineName != null ? pipelineName : "",
          processId != null ? processId : "",
          stageName != null ? stageName : "",
          cls != null ? cls.getName() : "");
      if (pipelineName != null) {
        metrics.pipeline(pipelineName).incrementInternalErrorCount();
      } else {
        metrics.incrementInternalErrorCount();
      }
      InternalErrorEntity internalErrorEntity = new InternalErrorEntity();
      internalErrorEntity.setErrorId(UUID.randomUUID().toString());
      internalErrorEntity.setErrorTime(ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS));
      internalErrorEntity.setServiceName(serviceName);
      internalErrorEntity.setPipelineName(pipelineName);
      internalErrorEntity.setProcessId(processId);
      internalErrorEntity.setStageName(stageName);
      internalErrorEntity.setClassName(cls.getName());
      internalErrorEntity.setErrorMessage(exception.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      exception.printStackTrace(pw);
      internalErrorEntity.setErrorLog(sw.toString());
      return repository.save(internalErrorEntity);
    } catch (Exception ignored) {
    }
    return null;
  }
}
