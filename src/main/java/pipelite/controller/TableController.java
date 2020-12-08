package pipelite.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = "/report")
@Tag(name = "TableAPI", description = "Reports tables")
public class TableController {

    @RequestMapping("/schedules")
    @Operation(description = "Table of all schedules")
    public String schedules() {
        return "schedules";
    }

    @RequestMapping("/processes")
    @Operation(description = "Table of all processes")
    public String processes() {
        return "processes";
    }

}
