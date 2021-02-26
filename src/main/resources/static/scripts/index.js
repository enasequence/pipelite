function showSchedules() {
    showTab('schedulesTab');
}

function showPipelines() {
    showTab('pipelinesTab');
}

function showProcesses(pipelineName) {
    refreshProcesses(pipelineName);
    showTab('processesTab');
}

function showProcess(pipelineName, processId) {
    refreshProcess(pipelineName, processId);
    showTab('processTab');
}

function showServers() {
    showTab('serversTab');
}

function showAdmin() {
    showTab('adminTab');
}

function showTab(id) {
    console.log("showTab: " + id);
    $('#indexTabs a[href="#' + id + '"]').tab('show');
}
