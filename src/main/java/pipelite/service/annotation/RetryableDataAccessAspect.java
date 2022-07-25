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
package pipelite.service.annotation;

import javax.annotation.PostConstruct;
import lombok.extern.flogger.Flogger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import pipelite.configuration.RetryableDataSourceConfiguration;
import pipelite.retryable.RetryableDataAccessAction;

@Aspect
@Order(Ordered.HIGHEST_PRECEDENCE)
@Component
@Configurable
@Flogger
public class RetryableDataAccessAspect {

  @Autowired RetryableDataSourceConfiguration configuration;
  RetryableDataAccessAction retryable;

  @PostConstruct
  private void init() {
    retryable = new RetryableDataAccessAction(configuration);
  }

  /**
   * Matches the execution of any public method in a type with the PipeliteTransactional annotation,
   * or any subtype of a type with the PipeliteTransactional annotation.
   */
  @Pointcut(
      "execution(public * ((@pipelite.service.annotation.RetryableDataAccess *)+).*(..)) && within(@pipelite.service.annotation.RetryableDataAccess *)")
  private void executionOfAnyPublicMethodInType() {}

  /** Matches the execution of any method with the PipeliteTransactional annotation. */
  @Pointcut("execution(@pipelite.service.annotation.RetryableDataAccess * *(..))")
  private void executionOfAnnotatedMethod() {}

  @Around("executionOfAnyPublicMethodInType() || executionOfAnnotatedMethod()")
  public Object pipeliteTransactional(ProceedingJoinPoint joinPoint) throws Throwable {
    return retryable.execute(() -> joinPoint.proceed());
  }
}
