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
package pipelite.service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.InternalErrorEntity;
import pipelite.metrics.PipeliteMetrics;
import pipelite.repository.InternalErrorRepository;
import pipelite.service.annotation.RetryableDataAccess;

@Service
@RetryableDataAccess
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Flogger
public class InternalErrorService {

  private final InternalErrorRepository repository;
  private final PipeliteMetrics metrics;
  private final String serviceName;

  public InternalErrorService(
      @Autowired PipeliteConfiguration pipeliteConfiguration,
      @Autowired InternalErrorRepository repository,
      @Autowired PipeliteMetrics metrics) {
    this.repository = repository;
    this.metrics = metrics;
    this.serviceName = pipeliteConfiguration.service().getName();
  }

  public InternalErrorEntity saveInternalError(Class cls, Throwable exception) {
    return saveInternalError(null, null, null, cls, exception);
  }

  public InternalErrorEntity saveInternalError(
      String pipelineName, String processId, String stageName, Class cls, Throwable exception) {
    try {
      StringWriter stackTrace = new StringWriter();
      exception.printStackTrace(new PrintWriter(stackTrace));
      log.atSevere().withCause(exception).log(
          "Internal error in service: %s, pipeline: %s, process: %s, stage: %s, class %s\n%s",
          serviceName != null ? serviceName : "?",
          pipelineName != null ? pipelineName : "?",
          processId != null ? processId : "?",
          stageName != null ? stageName : "?",
          cls != null ? cls.getName() : "?",
          stackTrace);
      metrics.error().incrementCount();

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
