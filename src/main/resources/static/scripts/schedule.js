$(document).ready(function () {
    $('#scheduleTable').DataTable({
        "ordering": false,
        columns: [
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

    autocompleteScheduleNamesText("scheduleScheduleName");

    // Restore state after refresh.
    let scheduleName = localStorage.getItem('scheduleScheduleName');
    if (scheduleName) {
        refreshSchedule(scheduleName);
    }
});

function refreshSchedule(scheduleName) {
    if (scheduleName) {
        $("#scheduleScheduleName").val(scheduleName);
    } else {
        scheduleName = $("#scheduleScheduleName").val();
    }

    if (scheduleName) {
        localStorage.setItem('scheduleScheduleName', scheduleName);
        $("#scheduleScheduleNameAlert").hide();

        let scheduleTable = $("#scheduleTable").DataTable();
        let url = setScheduleNameBadgeAndGetProcessUrl("scheduleScheduleNameBadge", scheduleName);

        $.get(url, function (data, status) {
            scheduleTable.clear().rows.add(data).draw();
        });
    } else {
        $("#scheduleScheduleNameAlert").text("Please provide schedule name");
        $("#scheduleScheduleNameAlert").show();
    }
}
