$(document).ready(function () {
    $('#processesTable').DataTable({
        columns: [
            {data: "pipelineName"},
            {
                data: "processId",
                render: function (data, type, row) {
                    let onClick = "showProcess('" + row.pipelineName + "','" + row.processId + "')";
                    // console.log(onClick);
                    return '<button type="button" class="btn btn-link" onclick="' + onClick + '">' + row.processId + '</button>';
                }
            },
            {data: "state"},
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
        dom: 'frtBip',
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

    autocompletePipelineNamesText("processesPipelineName");

    // Restore state.
    let pipelineName = getTabParams("processesParam");
    if (pipelineName) {
        refreshProcesses(pipelineName);
    }
});

function refreshProcesses(pipelineName) {

    if (pipelineName) {
        $("#processesPipelineName").val(pipelineName);
    } else {
        pipelineName = $("#processesPipelineName").val();
    }

    if (pipelineName) {
        setTabParams("processesParams", pipelineName);

        $("#processesPipelineNameAlert").hide();

        let processesTable = $('#processesTable').DataTable();
        let url = setPipelineNameBadgeAndGetProcessUrl("processesPipelineNameBadge", pipelineName);

        $.get(url, function (data, status) {
            processesTable.clear().rows.add(data).draw();
        });
    } else {
        $("#processesPipelineNameAlert").text("Please provide pipeline name");
        $("#processesPipelineNameAlert").show();
    }
}
