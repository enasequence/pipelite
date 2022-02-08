function setTabParams(name, value) {
    let searchParams = new URLSearchParams(document.location.search);
    searchParams.set(name, value);
    // Set to URL.
    history.replaceState({}, '', '?' + searchParams.toString());
    // Set to local storage.
    localStorage.setItem(name, value);
}

function getTabParams(name) {
    // Restore from URL.
    let searchParams = new URLSearchParams(document.location.search);
    let value = searchParams.get(name);
    if (!value) {
        // Restore from local storage.
        value = localStorage.getItem(name);
    }
    return value;
}

function showSchedule(scheduleName) {
    refreshSchedule(scheduleName);
    showTab('scheduleTab');
}

function showProcesses(pipelineName) {
    refreshProcesses(pipelineName);
    showTab('processesTab');
}

function showProcess(pipelineName, processId) {
    refreshProcess(pipelineName, processId);
    showTab('processTab');
}

function showTab(id) {
    console.log("showTab: " + id);
    $('#indexTabs a[href="#' + id + '"]').tab('show');
}

function autocompletePipelineNamesText(textId) {
    let url = "/pipelite/ui/api/pipeline/";
    autocompleteText(textId, url);
}

function autocompleteScheduleNamesText(textId) {
    let url = "/pipelite/ui/api/schedule/";
    autocompleteText(textId, url);
}

function autocompleteProcessNamesText(textId) {
    let url = "/pipelite/ui/api/process/";
    autocompleteText(textId, url);
}

function autocompleteText(textId, url) {
    $.get(url, function (data) {
        let pipelineNames = data.map(function (obj) {
            return obj.pipelineName;
        });

        function onlyUnique(value, index, self) {
            return self.indexOf(value) === index;
        }

        pipelineNames = pipelineNames.filter(onlyUnique);
        $("#" + textId).autocomplete({
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
}

function setPipelineNameBadgeAndGetProcessUrl(pipelineNameBadgeId, pipelineName, processIdBadgeId, processId) {
    let pipelineNameBadgeLabel = "Pipeline";
    let pipelineNameBadgeText;
    if (pipelineName) {
        pipelineNameBadgeText = pipelineNameBadgeLabel + ":" + pipelineName;
    } else {
        pipelineNameBadgeText = pipelineNameBadgeLabel + ":All";
    }
    $("#" + pipelineNameBadgeId).text(pipelineNameBadgeText);

    let processIdBadgeText = "Process:" + processId;
    $("#" + processIdBadgeId).text(processIdBadgeText);

    let url = "/pipelite/ui/api/process/";
    if (pipelineName && processId) {
        // Process from the database.
        url = url + pipelineName + "/" + processId + "/";
    } else if (pipelineName) {
        // Running processes.
        url = url + "?pipelineName=" + pipelineName + "&";
    }

    console.log(url);
    return url;
}

function setScheduleNameBadgeAndGetProcessUrl(scheduleNameBadgeId, scheduleName) {
    let scheduleNameBadgeLabel = "Schedule";
    let scheduleNameBadgeText;
    if (scheduleName) {
        scheduleNameBadgeText = scheduleNameBadgeLabel + ":" + scheduleName;
    } else {
        return;
    }
    $("#" + scheduleNameBadgeId).text(scheduleNameBadgeText);

    // All processes from the database.
    let url = "/pipelite/ui/api/process/" + scheduleName;
    console.log(url);
    return url;
}