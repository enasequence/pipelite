$(document).ready(function () {
    $('#processTable').DataTable({
        columns: [
            // { data: "pipelineName" },
            // { data: "processId" },
            {
                data: "state",
                render: function (data) {
                    if (data == 'FAILED') {
                        $("#processRetry").show();
                    } else {
                        $("#processRetry").hide();
                    }
                    return data;
                }
            },
            {
                data: "startTime",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD hh:mm:ss');
                    } else {
                        return '';
                    }
                }
            },
            {
                data: "endTime",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD hh:mm:ss');
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
            {
                data: "startTime",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD hh:mm:ss');
                    } else {
                        return '';
                    }
                }
            },
            {
                data: "endTime",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD hh:mm:ss');
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
        let pipelineName = $("#pipelineName").val();
        let processId = $("#processId").val();

        let table = $('#processStagesStatusTable').DataTable();
        let data = table.row($(this).parents('tr')).data();
        let url = "/pipelite/ui/api/log/" + pipelineName + "/" + processId + "/" + data.stageName + "/";
        console.log(url);
        $.get(url, function (data, status) {
            $("#logsText").text(data.log);
            $('#logsModal').modal('show');
        });
    });

    // Set up autocomplete for pipeline name.
    let url = "/pipelite/ui/api/process/run";
    $.get(url, function (data) {

        let pipelineNames = data.map(function (obj) {
            return obj.pipelineName;
        });

        function onlyUnique(value, index, self) {
            return self.indexOf(value) === index;
        }

        pipelineNames = pipelineNames.filter(onlyUnique);

        $("#pipelineName").autocomplete({
            source: pipelineNames,
            minLength: 0,
            open: function () {
                $(this).removeClass("ui-corner-all").addClass("ui-corner-top");
            },
            close: function () {
                $(this).removeClass("ui-corner-top").addClass("ui-corner-all");
            }
        });
    });

    // Show tables and graph.
    let urlParams = new URLSearchParams(window.location.search);
    let pipelineName = urlParams.get("pipelineName");
    let processId = urlParams.get("processId");
    // console.log("url parameter pipelineName:" + pipelineName);
    // console.log("url parameter processId:" + processId);
    if (pipelineName && processId) {
        $("#pipelineName").val(pipelineName);
        $("#processId").val(processId);
        refreshProcess();
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
        $("#pipelineName").val(pipelineName);
    }
    if (processId) {
        $("#processId").val(processId);
    }

    pipelineName = $("#pipelineName").val();
    processId = $("#processId").val();

    if (pipelineName && processId) {
        $("#processAlert").hide();

        // Show the filter badges.
        let pipelineNameBadgeText;
        let processIdBadgeText;
        if (pipelineName) {
            pipelineNameBadgeText = "Pipeline:" + pipelineName;
        } else {
            pipelineNameBadgeText = "";
        }
        if (processId) {
            processIdBadgeText = "Process:" + processId;
        } else {
            processIdBadgeText = "";
        }
        $("#processPipelineNameBadge").text(pipelineNameBadgeText);
        $("#processProcessIdBadge").text(processIdBadgeText);

        // Show the tables.
        let processTable = $('#processTable').DataTable();
        let processStagesStatusTable = $('#processStagesStatusTable').DataTable();
        let processStagesDataTable = $('#processStagesDataTable').DataTable();

        let processUrl = "/pipelite/ui/api/process/" + pipelineName + "/" + processId + "/";
        console.log(processUrl);
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
    let pipelineName = $("#pipelineName").val();
    let processId = $("#processId").val();
    let minWidth = 100;
    let fullWidth = $("#stagesGraph").width();
    if (fullWidth < minWidth) {
        return;
    }
    let fullHeight = 500;
    $("#stagesGraph").height(fullHeight);

    let url = "/pipelite/ui/api/stage/" + pipelineName + "/" + processId + "/";
    console.log(url);
    $.get(url, function (data, status) {
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

        let svg = d3.select("#stagesGraph").append("svg")
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
};

function processRetry() {
    let pipelineName = $("#pipelineName").val();
    let processId = $("#processId").val();

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
            $("#processRetryAlert").text("Failed to retry process. " + message);
            $("#processRetryAlert").show();
        },
        success: function () {
            refreshProcess();
        }
    });
}
