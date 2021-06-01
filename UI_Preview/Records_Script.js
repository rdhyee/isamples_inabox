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

//table ---------------------------------------------------------------------
//show specified identifier record
async function showRawRecord(id) {
    var url = `https://mars.cyverse.org/thing/${id}?full=false`;
    console.log(url);
    fetch(url)
        .then(response => response.json())
        .then(doc => {
            var e = document.getElementById("records_show");
            e.innerHTML = prettyPrintJson.toHtml(doc, FormatOptions = {
                indent: 2
            });
        })
}

//load records types
async function showTypes() {
    const url = "https://mars.cyverse.org/thing/types";
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
    showRawRecord(id);
}

//filter the records accordig users' input
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
    ajaxURL: "https://mars.cyverse.org/thing/?offset=0&limit=1000&status=200",
    paginationDataSent: {
        "page": "offset",
        "size": "limit",
    },

    ajaxRequesting: function(url, params) {
        console.log(url);
        console.log(params);
        return `https://mars.cyverse.org/thing/?offset=${params.offset}&limit=${params.limit}&status=200`;
    },
    ajaxProgressiveLoad: "scroll",
    paginationSize: 1000,
    ajaxParams: {
        key1: "id",
        keys: "tcreated"
    },
    columns: [{
        "title": "Id",
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
//table ---------------------------------------------------------------------


//enable input submit when users press enter
var inputText = document.getElementById("id_filter");
inputText.addEventListener("keyup", function(event) {
    if (event.keyCode === 13) {
        event.preventDefault();
        document.getElementById("bt_filter").click();
    }
})

//add types in a popup box
var modal = document.getElementsByClassName("types")[0];
var bt_types = document.getElementById("bt_types");
var closeBt = document.getElementsByClassName("closeBt")[0];

bt_types.addEventListener('click', openTypes);
closeBt.addEventListener('click', closeTypes);

function openTypes() {
    modal.style.display = "block";
}

function closeTypes() {
    modal.style.display = "none";
}