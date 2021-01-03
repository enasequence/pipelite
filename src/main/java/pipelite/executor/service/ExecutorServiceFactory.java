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
package pipelite.executor.service;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class ExecutorServiceFactory implements ApplicationContextAware {

  private static ApplicationContext context;

  /**
   * Returns the executor service if it exists. Returns null otherwise.
   *
   * @param cls the executor service class
   * @return the executor service if it exists. Returns null otherwise.
   */
  public static <T extends ExecutorService> T service(Class<T> cls) {
    return context.getBean(cls);
  }

  @Override
  public void setApplicationContext(ApplicationContext context) throws BeansException {
    ExecutorServiceFactory.context = context;
  }
}
