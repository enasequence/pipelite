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
package pipelite.metrics.helper;

public class MicroMeterHelper {

  public static String[] pipelineTags(String pipelineName) {
    return new String[] {"pipelineName", pipelineName};
  }

  public static String[] pipelineTags(String pipelineName, String state) {
    return new String[] {"pipelineName", pipelineName, "state", state};
  }

  public static String[] stageTags(String pipelineName, String stageName) {
    return new String[] {"pipelineName", pipelineName, "stageName", stageName};
  }

  public static String[] stageTags(String pipelineName, String stageName, String state) {
    return new String[] {"pipelineName", pipelineName, "stageName", stageName, "state", state};
  }
}
