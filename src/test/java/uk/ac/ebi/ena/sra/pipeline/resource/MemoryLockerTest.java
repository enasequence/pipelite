/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.resource;

import org.junit.Assert;
import org.junit.Test;

public class MemoryLockerTest {

  @Test
  public void Test() {
    MemoryLocker ml = new MemoryLocker();
    ProcessResourceLock rl = new ProcessResourceLock("pipelineName", "1");
    Assert.assertTrue(ml.lock(new ProcessResourceLock("pipelineName", "1")));
    Assert.assertTrue(ml.is_locked(new ProcessResourceLock("pipelineName", "1")));
    Assert.assertTrue(ml.lock(new ProcessResourceLock("pipelineName", "2")));
    Assert.assertTrue(ml.is_locked(new ProcessResourceLock("pipelineName", "2")));
    Assert.assertTrue(ml.unlock(rl));
    Assert.assertFalse(ml.is_locked(new ProcessResourceLock("pipelineName", "1")));
    Assert.assertTrue(ml.is_locked(new ProcessResourceLock("pipelineName", "2")));
  }
}
