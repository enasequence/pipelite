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
package pipelite.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = "/ui")
public class UIController {

  @RequestMapping("/schedules")
  public String schedules() {
    return "schedules";
  }

  @RequestMapping("/pipelines")
  public String pipelines() {
    return "pipelines";
  }

  @RequestMapping("/processes")
  public String processes() {
    return "processes";
  }

  @RequestMapping("/stages")
  public String stages() {
    return "stages";
  }

  @RequestMapping("/servers")
  public String servers() {
    return "servers";
  }

  @RequestMapping("/admin")
  public String admin() {
    return "admin";
  }
}
