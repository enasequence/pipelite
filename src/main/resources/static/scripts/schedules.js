$(document).ready(function () {
    $('#schedulesTable').DataTable({
        columns: [
            {data: "pipelineName"},
            {data: "cron"},
            {data: "description"},
            {
                data: "processId",
                render: function (data, type, row) {
                    if (row.processId) {
                        let onClick = "showProcess('" + row.pipelineName + "','" + row.processId + "')";
                        // console.log(onClick);
                        return '<button type="button" class="btn btn-link" onclick="' + onClick + '">' + row.processId + '</button>';
                    }
                    return '';
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
                data: "nextTime",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD HH:mm:ss');
                    } else {
                        return '';
                    }
                }
            },
            {
                data: "lastCompleted",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD HH:mm:ss');
                    } else {
                        return '';
                    }
                }
            },
            {
                data: "lastFailed",
                render: function (data) {
                    if (data) {
                        return moment(data).format('YYYY/MM/DD HH:mm:ss');
                    } else {
                        return '';
                    }
                }
            }
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

    refreshSchedules();
});

function refreshSchedules() {
    let schedulesTable = $('#schedulesTable').DataTable();

    let url = "/pipelite/ui/api/schedule/";
    console.log(url);
    $.get(url, function (data, status) {
        schedulesTable.clear().rows.add(data).draw();
    });
};
