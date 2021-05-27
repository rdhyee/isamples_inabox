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
            if (e) {
                e.remove();
            }
            typeTable(types);
        });
}

//create types table
function typeTable(data) {
    var container = document.getElementById("types_table");
    var table = document.createElement("table");
    table.id = "types_show";
    var rowName = table.insertRow();
    var name = rowName.insertCell();
    name.innerHTML = "Name";
    name.scope = "header";
    for (var i = 0; i < data.length; i++) {
        var eName = rowName.insertCell();
        eName.innerHTML = data[i].item_type;
    }
    var rowCount = table.insertRow();
    var count = rowCount.insertCell();
    count.innerHTML = "Count";
    count.scope = "header";
    for (var j = 0; j < data.length; j++) {
        var eCount = rowCount.insertCell();
        eCount.innerHTML = data[j].count;
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
    var e1 = document.getElementById("records_load");
    var e2 = document.getElementById("total_records");
    console.log(response);
    e1.innerHTML = parseInt(e1.innerHTML) + response.data.length;
    e2.innerHTML = response.total_records;
    showTypes();
    return response;
}
//load records from server into tabulator
var table = new Tabulator("#records_table", {
    layout: "fitColumns",
    height: 300,
    placeholder: "No data availble",
    ajaxURL: "https://mars.cyverse.org/thing/?offset=0&limit=1000&status=200",
    paginationDataSent: {
        "page": "offset",
        "size": "limit",
    },
    ajaxURLGenerator: function(url, config, params) {
        return url + `?offset=${params.offset}&limit=${params.limit}&status=200`;
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
    ajaxSorting: true,
    selectable: 1
});

//add split bar horizonal and vertical
Split(["#records_table", "#bottom"], {
    gutterSize: 30,
    direction: "vertical",
    sizes: [30, 70],
});
Split(["#left", "#right"], {
    gutterSize: 10,
});

//enable input submit when users press enter
var inputText = document.getElementById("id_filter");
inputText.addEventListener("keyup", function(event) {
    if (event.keyCode === 13) {
        event.preventDefault();
        document.getElementById("bt_fillter").click();
    }
})

//add types in a popup box
var modal = document.getElementById("types");
var bt_types = document.getElementById("bt_types");
var closeBt = document.getElementById("close");

bt_types.addEventListener('click', openTypes);
closeBt.addEventListener('click', closeTypes);

function openTypes() {
    modal.style.display = "block";
}

function closeTypes() {
    modal.style.display = "none";
}