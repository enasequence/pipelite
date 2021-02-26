$(document).ready(function () {
    $('#processesTables').DataTable({
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

    // Set up autocomplete for pipeline name.
    let url = "/pipelite/ui/api/process/";
    $.get(url, function (data) {
        let pipelineNames = data.map(function (obj) {
            return obj.pipelineName;
        });

        function onlyUnique(value, index, self) {
            return self.indexOf(value) === index;
        }

        pipelineNames = pipelineNames.filter(onlyUnique);

        $("#processesPipelineName").autocomplete({
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
});

function refreshProcesses(pipelineName) {
    if (pipelineName) {
        $("#processesPipelineName").val(pipelineName);
    }

    pipelineName = $("#processesPipelineName").val();

    if (pipelineName) {
        let processesTables = $('#processesTables').DataTable();

        // Show the filter badge.
        let pipelineNameBadgeText;
        if (pipelineName) {
            pipelineNameBadgeText = "Pipeline:" + pipelineName;
        } else {
            pipelineNameBadgeText = "";
        }
        $("#processesPipelineNameBadge").text(pipelineNameBadgeText);

        let url = "/pipelite/ui/api/process/";
        if (pipelineName) {
            url = url + "?pipelineName=" + pipelineName + "&";
        }
        console.log(url);
        $.get(url, function (data, status) {
            processesTables.clear().rows.add(data).draw();
        });
    }
};
