
const PAGE_SIZE = 50;

//create split bar for table and content
Split(["#records_table", "#info"], {
    direction: "vertical",
    sizes: [30, 70],
    gutterSize: 30,
});

var infoSplit = Split(["#InfoLeft", "#InfoRight"], {
    gutterSize: 10,
    minSize: [200, 200],
})

/* Disable this for now
//log out
var logout = document.getElementById("logout");
logout.addEventListener('click', function() {
    location.href = "./login.html";
})
 */

//change the raw data and iSample data panels directions
function changePanes(direction) {
    var container = document.getElementsByClassName("infoContainer")[0];
    if (direction == 'h') {
        container.style.flexDirection = "row";
        infoSplit.destroy();
        infoSplit = Split(["#InfoLeft", "#InfoRight"], {
            gutterSize: 10,
            minSize: [200, 200],
        })
    } else {
        container.style.flexDirection = "column";
        infoSplit.destroy();
        infoSplit = Split(["#InfoLeft", "#InfoRight"], {
            gutterSize: 10,
            minSize: [100, 100],
            direction: "vertical",
        })
    }
}

// navigate to the specified identifier
function navigateTo(identifier) {
    const url = `/thing/${id}?format=full`
    fetch(url).then(response => response.json()).then(doc => {
        var offset = doc._id;
        var page = Math.floor(offset / PAGE_SIZE);
    });
}

//table ---------------------------------------------------------------------
//show specified identifier record
async function showRawRecord(id) {
    const raw_url = `/thing/${id}?format=original`;
    const xform_url = `/thing/${id}?format=core`;
    await Promise.all([
        fetch(raw_url)
            .then(response => response.json())
            .then(doc => {
                var e = document.getElementById("record_original");
                e.innerHTML = prettyPrintJson.toHtml(doc, FormatOptions = {
                    indent: 2
                });
            }),
        fetch(xform_url)
            .then(response => response.json())
            .then(doc => {
                var e = document.getElementById("record_xform");
                e.innerHTML = prettyPrintJson.toHtml(doc, FormatOptions = {
                    indent: 2
                });
            })
    ])
}

//load records types
async function showTypes() {
    const url = "/thing/types";
    fetch(url)
        .then(res => res.json())
        .then(types => {
            var e = document.getElementById("types_show");
            if (e) e.remove();
            typeTable(types);
        });
}

//create types table
function typeTable(data) {
    var container = document.getElementById("types_table");
    var table = document.createElement("table");
    var caption = table.createCaption();
    caption.innerHTML = "<h3>Types</h3>";
    table.id = "types_show";
    var i = 0;
    var rowName;
    var name;
    var rowCount;
    var count;
    while (i < data.length) {
        if (i % (data.length / 4 | 0) == 0) {
            rowName = table.insertRow();
            name = rowName.insertCell();
            rowCount = table.insertRow();
            count = rowCount.insertCell();
            name.innerHTML = "Name";
            name.scope = "header";
            count.innerHTML = "Count";
            count.scope = "header";
        }
        var eName = rowName.insertCell();
        eName.innerHTML = data[i].item_type;
        var eCount = rowCount.insertCell();
        eCount.innerHTML = data[i].count;
        i++;
    }
    container.appendChild(table);
}

//select row and show record informaton
function rowClick(e, row) {
    console.log(row._row);
    var id = row._row.data.id;
    var reportId = document.getElementById('currentID');
    reportId.value = id;
    reportId.innerHTML = id;
    showRawRecord(id);
}

//filter the records according users' input
function doIdFilter(eid) {
    var e = document.getElementById(eid).value;
    console.log("Filter ID: " + e);
    table.setFilter("id", "like", e);
}

//update load data
function Dataloaded(url, params, response) {
    var e1 = document.getElementById("records_loaded");
    var e2 = document.getElementById("total_records");
    console.log(response);
    e1.innerHTML = parseInt(e1.innerHTML) + response.data.length;
    e2.innerHTML = response.total_records;
    showTypes()
    return response;
}

//load records from server into tabulator
var table = new Tabulator("#records_table", {
    layout: "fitColumns",
    placeholder: "No data availble",
    ajaxURL: "/thing/",
    paginationDataSent: {
        "page": "offset",
        "size": "limit",
    },
    ajaxRequesting: function(url, params) {
        console.log(url);
        console.log(params);
        params.offset = "" + (parseInt(params.offset) - 1) * parseInt(params.limit);
        delete params.key1;
        delete params.keys;
    },
    ajaxProgressiveLoad: "scroll",
    pagination: "remote",
    paginationSize: PAGE_SIZE,
    ajaxParams: {
        key1: "id",
        keys: "tcreated"
    },
    index: "_id",
    columns: [{
        "title":"id",
        "field":"_id",
        "width": 10
    }, {
        "title": "Identifier",
        field: "id",
        width: 500
    }, {
        "title": "Authority Id",
        field: "authority_id",
        width: 150
    }, {
        "title": "Time Created",
        field: "tcreated",
        width: 300
    }, {
        "title": "status",
        field: "resolved_status",
        width: 150
    }, {
        "title": "url",
        field: "resolved_url"
    }, {
        "title": "elapsed",
        field: "resolve_elapsed",
        width: 150
    }],
    ajaxResponse: Dataloaded,
    rowClick: rowClick,
    selectable: 1
});

//enable input submit when users press enter
var inputText = document.getElementById("id_filter");
inputText.addEventListener("keyup", function(event) {
        if (event.keyCode === 13) {
            event.preventDefault();
            document.getElementById("bt_filter").click();
        }
    })
    //table ---------------------------------------------------------------------

//type popup-----------------------------------------------------------------


//add types in a popup box
var modal = document.getElementsByClassName("types")[0];
var bt_types = document.getElementById("bt_types");
var closeBt = document.getElementsByClassName("closeBt")[0];

bt_types.addEventListener('click', function() { modal.style.display = "block"; });
closeBt.addEventListener('click', function() { modal.style.display = "none"; });



//report popup-----------------------------------------------------------------
var reportModel = document.getElementsByClassName("report")[0];
var bt_report = document.getElementsByClassName("bt_report")[0];
var repClose = document.getElementById("repClose");
var repText = document.getElementsByClassName("retext")[0];

bt_report.addEventListener("click", function() {
    reportModel.style.display = "block";
    repText.value = "";
    repText.placeholder = "Please enter bug";
});
repClose.addEventListener("click", function() {
    reportModel.style.display = "none";
})

var reportTitle = document.getElementById('currentID');
var reportBody = document.getElementById('reportBody');
var bt_issue = document.getElementById('bt_issue');
bt_issue.addEventListener('click', createIssue);
async function createIssue() {
    if (reportTitle.value == undefined) {
        alert("Please choose a record!");
    } else {
        const url = `http://localhost:2400/issues?title=${reportTitle.value}&report=${reportBody.value}`
        fetch(url)
            .then(res => { console.log("success") })

        reportModel.style.display = "none";
    }
}


window.onclick = function(event) {
    if (event.target == modal) {
        modal.style.display = "none";
    }

    if (event.target == reportModel) {
        reportModel.style.display = "none";
    }
}