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
package uk.ac.ebi.ena.sra.pipeline.mock.stages;

import uk.ac.ebi.ena.sra.pipeline.launcher.iface.StageTask;

public class FirstStage implements StageTask {

  @Override
  public void init(Object id, boolean is_forced) throws Throwable {
    System.out.println(
        String.format(
            "Init for %s, id: %s, forced: %s ", this.getClass().getSimpleName(), id, is_forced));
  }

  @Override
  public void execute() throws Throwable {
    System.out.println(String.format("Execute for %s", this.getClass().getSimpleName()));
  }

  @Override
  public void unwind() {
    System.out.println(String.format("Unwind for %s", this.getClass().getSimpleName()));
  }
}
