package pipelite.controller;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = "/ui")
public class UIController {

    @RequestMapping("/processes")
    public String processes() {
        return "processes";
    }

    @RequestMapping("/schedules")
    public String schedules() {
        return "schedules";
    }

    @RequestMapping("/services")
    public String services() {
        return "services";
    }

    @RequestMapping("/admin")
    public String admin() {
        return "admin";
    }
}
