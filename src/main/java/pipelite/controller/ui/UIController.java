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
package pipelite.controller.ui;

import io.swagger.v3.oas.annotations.Hidden;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = "/ui")
@Hidden
public class UIController {

  @RequestMapping("/login")
  /** Uses html template. */
  public String login() {
    return "login";
  }

  @RequestMapping("/schedules")
  /** Uses html template. */
  public String schedules() {
    return "schedules";
  }

  @RequestMapping("/pipelines")
  /** Uses html template. */
  public String pipelines() {
    return "pipelines";
  }

  @RequestMapping("/processes")
  /** Uses html template. */
  public String processes() {
    return "processes";
  }

  @RequestMapping("/process")
  /** Uses html template. */
  public String stages() {
    return "process";
  }

  @RequestMapping("/servers")
  /** Uses html template. */
  public String servers() {
    return "servers";
  }

  @RequestMapping("/admin")
  /** Uses html template. */
  public String admin() {
    return "admin";
  }

  @RequestMapping(value = "/logout")
  public String logout(HttpServletRequest request, HttpServletResponse response) {
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    if (auth != null) {
      new SecurityContextLogoutHandler().logout(request, response, auth);
    }
    return "redirect:/ui/login?logout";
  }
}
