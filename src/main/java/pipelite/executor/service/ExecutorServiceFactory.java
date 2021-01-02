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
