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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import pipelite.service.InternalErrorService;

public class InternalErrorHandlerTest {

  private static final String SERVICE_NAME = "test1";
  private static final String ERROR_MESSAGE = "test2";
  private static final String RECOVER_ERROR_MESSAGE = "test3";
  private int internalErrorCnt = 0;

  @Test
  public void test() {
    InternalErrorService internalErrorService = mock(InternalErrorService.class);
    doAnswer(
            invocation -> {
              internalErrorCnt++;
              String serviceName = invocation.getArgument(0);
              Exception exception = invocation.getArgument(5);
              assertThat(serviceName).isEqualTo(SERVICE_NAME);
              assertThat(exception).isInstanceOf(RuntimeException.class);
              assertThat(exception.getMessage()).isEqualTo(ERROR_MESSAGE);
              return null;
            })
        .when(internalErrorService)
        .saveInternalError(any(), any(), any(), any(), any());

    InternalErrorHandler handler =
        new InternalErrorHandler(internalErrorService, SERVICE_NAME, this);

    // Should not throw
    assertThat(handler.execute(() -> {})).isTrue();
    assertThat(internalErrorCnt).isEqualTo(0);

    // Should not throw
    assertThat(
            handler.execute(
                () -> {
                  throw new RuntimeException(ERROR_MESSAGE);
                },
                (ex) -> {
                  assertThat(ex).isInstanceOf(RuntimeException.class);
                  assertThat(ex.getMessage()).isEqualTo(ERROR_MESSAGE);
                }))
        .isFalse();
    assertThat(internalErrorCnt).isEqualTo(1);

    // Should not throw
    assertThat(
            handler.execute(
                () -> {
                  throw new RuntimeException(ERROR_MESSAGE);
                },
                (ex) -> {
                  assertThat(ex).isInstanceOf(RuntimeException.class);
                  assertThat(ex.getMessage()).isEqualTo(ERROR_MESSAGE);
                  throw new RuntimeException(RECOVER_ERROR_MESSAGE);
                }))
        .isFalse();
    assertThat(internalErrorCnt).isEqualTo(2);
  }
}
