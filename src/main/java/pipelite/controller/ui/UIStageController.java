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
package pipelite.controller.ui;

import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.web.bind.annotation.*;
import pipelite.controller.api.StageController;

@RestController
@RequestMapping(value = "/ui/api/stage")
@Hidden
/**
 * Extends the API controller to allow use of different authentication scheme. Not documented using
 * OpenAPI.
 */
public class UIStageController extends StageController {}
