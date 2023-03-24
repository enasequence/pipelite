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
            {data: "stageRunningCount"}
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
        // No state to restore.
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
}
