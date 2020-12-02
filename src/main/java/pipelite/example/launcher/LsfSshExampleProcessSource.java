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
package pipelite.example.launcher;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;
import pipelite.process.ProcessSource;

@Component
public class LsfSshExampleProcessSource implements ProcessSource {

  public static final int PROCESS_CNT = 10;

  private AtomicInteger nextCnt = new AtomicInteger();
  private AtomicInteger acceptCount = new AtomicInteger();
  private AtomicInteger rejectCount = new AtomicInteger();

  @Override
  public String getPipelineName() {
    return LsfSshExampleProcessFactory.PIPELINE_NAME;
  }

  @Override
  public NewProcess next() {
    if (nextCnt.getAndIncrement() < PROCESS_CNT) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        nextCnt.set(PROCESS_CNT);
      }
      String processId = String.valueOf(ZonedDateTime.now().toInstant().toEpochMilli());
      return new NewProcess(processId);
    } else {
      return null;
    }
  }

  @Override
  public void accept(String processId) {
    acceptCount.incrementAndGet();
  }

  public int getNextCnt() {
    return nextCnt.get();
  }

  public int getAcceptCount() {
    return acceptCount.get();
  }

  public int getRejectCount() {
    return rejectCount.get();
  }
}
