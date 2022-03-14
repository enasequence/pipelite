$(document).ready(function () {
    $('#pipelinesTable').DataTable({
        columns: [
            {data: "pipelineName"},
            {data: "maxRunningCount"},
            {
                data: "runningCount",
                render: function (data, type, row) {
                    let onClick = "showProcesses('" + row.pipelineName + "')";
                    // console.log(onClick);
                    return '<button type="button" class="btn btn-link" onclick="' + onClick + '">' + row.runningCount + '</button>';
                }
            }
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

    refreshPipelines();
});

// Show the table and filter badges.
function refreshPipelines() {
    let pipelinesTable = $('#pipelinesTable').DataTable();
    let pipelinesRunningHistoryType = $("#pipelinesRunningHistoryType").val();
    let runningHistorySince = $("#pipelinesRunningHistorySince").val();

    // Show the filter badges.
    let runningHistoryTypeText = $("#pipelinesRunningHistoryType option:selected").text();
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

    let url = "/pipelite/ui/api/pipeline/";
    console.log(url);
    $.get(url, function (data, status) {
        pipelinesTable.clear().rows.add(data).draw();
    });

    pipelinesPlot();
}

function pipelinesPlot() {
    let runningHistorySince = $("#pipelinesRunningHistorySince").val();
    let pipelinesRunningHistoryType = $("#pipelinesRunningHistoryType").val();
    let url = "/pipelite/ui/api/pipeline/run/history/plot?since=" + runningHistorySince + "&type=" + pipelinesRunningHistoryType + "&id=pipelinesRunningHistoryPlot";
    console.log(url);
    $.getScript(url,
        function (data) {
            // console.log( data );
        });
}
