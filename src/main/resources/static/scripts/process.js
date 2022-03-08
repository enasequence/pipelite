$(document).ready(function () {
    $("#retryProcess").prop("disabled", true);

    $('#processTable').DataTable({
        columns: [
            // { data: "pipelineName" },
            // { data: "processId" },
            {
                data: "state",
                render: function (data) {
                    if (data == 'FAILED') {
                        $("#retryProcess").prop("disabled", false);
                    }
                    return data;
                }
            },
            {
                data: "startTime",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD HH:mm:ss');
                    } else {
                        return '';
                    }
                }
            },
            {
                data: "endTime",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD HH:mm:ss');
                    } else {
                        return '';
                    }
                }
            },
            {data: "executionCount"},
            {data: "priority"}
        ],
        dom: 't',
        responsive: true,
        language: {
            "zeroRecords": " "
        },
    });

    $('#processStagesStatusTable').DataTable({
        columns: [
            {data: "pipelineName", visible: false, searchable: false},
            {data: "processId", visible: false, searchable: false},
            {data: "stageName"},
            {data: "stageState"},
            {data: "errorType"},
            {
                data: "startTime",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD HH:mm:ss');
                    } else {
                        return '';
                    }
                }
            },
            {
                data: "endTime",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD HH:mm:ss');
                    } else {
                        return '';
                    }
                }
            },
            {data: "executionTime"},
            {data: "executionCount"},
            {
                "data": null,
                "defaultContent": "<button class=\"btn btn-outline-secondary mr-2\">Logs</button>"
            }
        ],
        dom: 'tBip',
        buttons: {
            buttons: [
                {extend: 'copy', className: 'btn btn-outline-secondary'},
                {extend: 'csv', className: 'btn btn-outline-secondary'},
                {extend: 'excel', className: 'btn btn-outline-secondary'}
            ],
            dom: {
                button: {
                    className: 'btn'
                }
            }
        },
        orderCellsTop: true,
        fixedHeader: true,
        responsive: true,
        language: {
            "zeroRecords": " "
        },
    });

    $('#processStagesDataTable').DataTable({
        columns: [
            {data: "pipelineName", visible: false, searchable: false},
            {data: "processId", visible: false, searchable: false},
            {data: "stageName"},
            {data: "executorName"},
            {data: "executorData"},
            {data: "executorParams"},
            {data: "resultParams"},
        ],
        dom: 'tBip',
        buttons: {
            buttons: [
                {extend: 'copy', className: 'btn btn-outline-secondary'},
                {extend: 'csv', className: 'btn btn-outline-secondary'},
                {extend: 'excel', className: 'btn btn-outline-secondary'}
            ],
            dom: {
                button: {
                    className: 'btn'
                }
            }
        },
        orderCellsTop: true,
        fixedHeader: true,
        responsive: true,
        language: {
            "zeroRecords": " "
        },
    });

    // Logs button is pressed.
    $('#processStagesStatusTable tbody').on('click', 'button', function () {
        let pipelineName = $("#processPipelineName").val();
        let processId = $("#processProcessId").val();

        let table = $('#processStagesStatusTable').DataTable();
        let data = table.row($(this).parents('tr')).data();
        let url = "/pipelite/ui/api/log/" + pipelineName + "/" + processId + "/" + data.stageName + "/";
        console.log(url);
        $.get(url, function (data, status) {
            $("#logsText").text(data.log);
            $('#logsModal').modal('show');
        });
    });

    autocompleteProcessNamesText("processPipelineName");

    // Restore state from URL.

    let processParams = getTabParams("processParams");
    if (processParams) {
        let [pipelineName, processId] = processParams.split(",");
        if (pipelineName && processId) {
            refreshProcess(pipelineName, processId);
        }
    }

    // Show graph when tab is changed.
    $('#processTabs a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
        $('svg').remove();
        plotProcess();
    });
});

// Show the filter badges, tables and graph.
function refreshProcess(pipelineName, processId) {
    if (pipelineName) {
        $("#processPipelineName").val(pipelineName);
    } else {
        pipelineName = $("#processPipelineName").val();
    }

    if (processId) {
        $("#processProcessId").val(processId);
    } else {
        processId = $("#processProcessId").val();
    }

    if (pipelineName && processId) {
        setTabParams("processParams", pipelineName + "," + processId);

        $("#processPipelineNameAndProcessIdAlert").hide();

        let processUrl = setPipelineNameBadgeAndGetProcessUrl(
            "processPipelineNameBadge", pipelineName,
            "processProcessIdBadge", processId);

        // Show the tables.
        let processTable = $('#processTable').DataTable();
        let processStagesStatusTable = $('#processStagesStatusTable').DataTable();
        let processStagesDataTable = $('#processStagesDataTable').DataTable();

        $.get(processUrl, function (data, status) {
            processTable.clear().rows.add(data).draw();
        });

        let stagesUrl = "/pipelite/ui/api/stage/" + pipelineName + "/" + processId + "/";
        console.log(stagesUrl);
        $.get(stagesUrl, function (data, status) {
            processStagesStatusTable.clear().rows.add(data).draw();
            processStagesDataTable.clear().rows.add(data).draw();
        });

        plotProcess();
    } else {
        $("#processPipelineNameAndProcessIdAlert").text("Please provide pipeline name and process id");
        $("#processPipelineNameAndProcessIdAlert").show();
    }
};

function plotProcess() {
    let pipelineName = $("#processPipelineName").val();
    let processId = $("#processProcessId").val();
    let minWidth = 100;
    let fullWidth = $("#processGraph").width();
    if (fullWidth < minWidth) {
        return;
    }
    let fullHeight = 500;
    $("#processGraph").height(fullHeight);

    let url = "/pipelite/ui/api/stage/" + pipelineName + "/" + processId + "/";
    console.log(url);
    $.get(url, function (data) {
        let g = new dagreD3.graphlib.Graph()
            .setGraph({
                ranksep: 20,
                nodesep: 20,
                rankdir: "LR"
            })
            .setDefaultEdgeLabel(function () {
                return {};
            });

        // Set nodes
        data.forEach(function (stage) {
            if (stage.stageState == "SUCCESS") {
                g.setNode(stage.stageName, {label: stage.stageName, class: "stageSuccess"});
            } else if (stage.stageState == "ERROR") {
                g.setNode(stage.stageName, {label: stage.stageName, class: "stageError"});
            } else if (stage.stageState == "ACTIVE") {
                g.setNode(stage.stageName, {label: stage.stageName, class: "stageActive"});
            } else {
                g.setNode(stage.stageName, {label: stage.stageName});
            }
        });

        // Set edges
        data.forEach(function (stage) {
            stage.dependsOnStage.forEach(function (dependsOnStage) {
                g.setEdge(dependsOnStage, stage.stageName, {
                    // label: "depends on"
                });
            });
        });

        // Set some general styles
        g.nodes().forEach(function (v) {
            let node = g.node(v);
            node.rx = node.ry = 5;
        });

        // Add some custom colors based on state
        // g.node('CLOSED').style = "fill: #f77";
        // g.node('ESTAB').style = "fill: #7f7";

        let svg = d3.select("#processGraph").append("svg")
            .attr("width", fullWidth)
            .attr("height", fullHeight);
        let svgGroup = svg.append("g");

        let render = new dagreD3.render();
        render(d3.select("svg g"), g);

        let paddingPercent = 0.95;
        let svgGroupBounds = svgGroup.node().getBBox();
        let width = svgGroupBounds.width,
            height = svgGroupBounds.height;

        let midX = svgGroupBounds.x + width / 2,
            midY = svgGroupBounds.y + height / 2;
        let maxScale = 1;
        let minScale = 0.75;
        let scale = (paddingPercent || 0.75) / Math.max(width / fullWidth, height / fullHeight);
        if (scale > maxScale) {
            scale = maxScale;
        }
        if (scale < minScale) {
            scale = minScale;
        }
        let leftMargin = 10;
        let translate = [leftMargin /*fullWidth / 2 - scale * midX*/, fullHeight / 2 - scale * midY];
        // console.log("scale: " + scale);
        // console.log("translate: " + translate);
        svgGroup.attr('transform',
            'translate(' + translate + ')' +
            'scale(' + scale + ')');
    });
};

function processCopyLogs() {
    let range = document.createRange();
    range.selectNode(document.getElementById("logsText"));
    window.getSelection().removeAllRanges(); // clear current selection
    window.getSelection().addRange(range); // to select text
    document.execCommand("copy");
    window.getSelection().removeAllRanges();// to deselect
}

function retryProcess() {
    let pipelineName = $("#processPipelineName").val();
    let processId = $("#processProcessId").val();

    let url = "/pipelite/ui/api/action/process/retry/" + pipelineName + "/" + processId;
    console.log(url);

    $.ajax({
        url: url,
        type: 'PUT',
        data: '',
        error: function (context) {
            let message = "";
            try {
                message = JSON.parse(context.responseText)[0].message;
            } catch (err) {
            }
            $("#retryProcessAlert").text(message);
            $("#retryProcessAlert").show();
            $("#retryProcessInfo").hide();
        }, success: function () {
            $("#retryProcessInfo").text("Process " + pipelineName + " " + processId + " execution will start shortly");
            $("#retryProcessInfo").show();
            $("#retryProcessAlert").hide();

            refreshProcess();
        }
    });
}
