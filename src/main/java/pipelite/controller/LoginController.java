package pipelite.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = "/")
public class LoginController {

  @RequestMapping("/login")
  public String login() {
    return "login";
  }
}
