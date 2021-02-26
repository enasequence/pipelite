$(document).ready(function () {
    $('#serversTable').DataTable({
        ajax: {
            url: "/pipelite/ui/api/server/",
            dataSrc: ""
        },
        columns: [
            {
                data: "url",
                render: function (data) {
                    return '<a href="' + data + '" target="_blank">' + data + '</a>';
                }
            },
            {data: "healthy"}
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
});
