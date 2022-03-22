$(document).ready(function () {
    $('#pipelinesTable').DataTable({
        columns: [
            {data: "pipelineName"},
            {data: "maxProcessRunningCount"},
            {
                data: "processRunningCount",
                render: function (data, type, row) {
                    let onClick = "showProcesses('" + row.pipelineName + "')";
                    // console.log(onClick);
                    return '<button type="button" class="btn btn-link" onclick="' + onClick + '">' + row.processRunningCount + '</button>';
                }
            },
            {data: "stageRunningCount"},
            {data: "stageSubmitCount"}
            /*,
            {data: "pendingCount"},
            {data: "activeCount"},
            {data: "completedCount"},
            {data: "failedCount"}
             */
        ],
        dom: 'tip',
        orderCellsTop: true,
        fixedHeader: true,
        responsive: true,
        language: {
            "zeroRecords": " "
        },
    });

    // Restore state.
    let pipelinesParams = getState("pipelines");
    if (pipelinesParams) {
        let [historyType, historySince] = pipelinesParams.split(",");
        if (historyType && historySince) {
            $("#pipelinesRunningHistoryType").val(historyType);
            $("#pipelinesRunningHistorySince").val(historySince);
        }
    }

    refreshPipelines();
});

// Show the table.
function refreshPipelines() {
    let pipelinesTable = $('#pipelinesTable').DataTable();

    let url = "/pipelite/ui/api/pipeline/";
    console.log(url);
    $.get(url, function (data, status) {
        pipelinesTable.clear().rows.add(data).draw();
    });

    pipelinesPlot();
}

function pipelinesPlot() {
    let runningHistoryType = $("#pipelinesRunningHistoryType").val();
    let runningHistoryTypeText = $("#pipelinesRunningHistoryType option:selected").text();
    let runningHistorySince = $("#pipelinesRunningHistorySince").val();

    // Save state.
    setState("pipelines", runningHistoryType + "," + runningHistorySince);

    // Show the filter badges.
    $("#pipelinesRunningHistoryTypeBadge").text(runningHistoryTypeText);

    let runningHistorySinceText = "Last ";

    let minutes = runningHistorySince % 60;
    let hours = (runningHistorySince - minutes) / 60 % 24;
    let days = (runningHistorySince - minutes - hours * 60) / 60 / 24;

    if (days == 1) {
        runningHistorySinceText = runningHistorySinceText + days + " day ";
    }
    if (days > 1) {
        runningHistorySinceText = runningHistorySinceText + days + " days ";
    }
    if (hours == 1) {
        runningHistorySinceText = runningHistorySinceText + hours + " hour ";
    } else if (hours > 1) {
        runningHistorySinceText = runningHistorySinceText + hours + " hours ";
    }
    if (minutes == 1) {
        runningHistorySinceText = runningHistorySinceText + minutes + " minute";
    } else if (minutes > 1) {
        runningHistorySinceText = runningHistorySinceText + minutes + " minutes";
    }
    $("#pipelinesRunningHistorySinceBadge").text(runningHistorySinceText);

    // Show the plot.
    let pipelinesRunningHistoryType = $("#pipelinesRunningHistoryType").val();
    let url = "/pipelite/ui/api/pipeline/run/history/plot?since=" + runningHistorySince + "&type=" + pipelinesRunningHistoryType + "&id=pipelinesRunningHistoryPlot";
    console.log(url);
    $.getScript(url,
        function (data) {
            // console.log( data );
        });
}
