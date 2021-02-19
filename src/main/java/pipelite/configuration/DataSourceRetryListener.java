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
package pipelite.configuration;

import java.util.function.Consumer;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.listener.RetryListenerSupport;

@Configuration
@Flogger
public class DataSourceRetryListener extends RetryListenerSupport {

  private final DataSourceRetryConfiguration dataSourceRetryConfiguration;
  private Consumer<Throwable> closeThrowableLister;
  private Consumer<Throwable> onErrorThrowableLister;

  public DataSourceRetryListener(
      @Autowired DataSourceRetryConfiguration dataSourceRetryConfiguration) {
    this.dataSourceRetryConfiguration = dataSourceRetryConfiguration;
  }

  @Override
  public <T, E extends Throwable> void close(
      RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
    if (throwable != null) {
      log.atSevere().withCause(throwable).log("Terminal exception from service using data source");
    }
    super.close(context, callback, throwable);
    if (closeThrowableLister != null && throwable != null) {
      closeThrowableLister.accept(throwable);
    }
  }

  @Override
  public <T, E extends Throwable> void onError(
      RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
    super.onError(context, callback, throwable);
    if (dataSourceRetryConfiguration.recoverableException(throwable)) {
      log.atSevere().withCause(throwable).log(
          "Recoverable exception from service using data source");
    } else {
      log.atSevere().withCause(throwable).log(
          "Unrecoverable exception from service using data source");
    }
    if (onErrorThrowableLister != null && throwable != null) {
      onErrorThrowableLister.accept(throwable);
    }
  }

  public void setCloseThrowableLister(Consumer<Throwable> closeThrowableLister) {
    this.closeThrowableLister = closeThrowableLister;
  }

  public void setOnErrorThrowableLister(Consumer<Throwable> onErrorThrowableLister) {
    this.onErrorThrowableLister = onErrorThrowableLister;
  }
}
