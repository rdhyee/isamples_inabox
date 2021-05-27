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
    return response;
}

function tableAndSplit() {
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
            "title": "id",
            field: "id",
        }, {
            "title": "authority_id",
            field: "authority_id",
            width: 150
        }, {
            "title": "time created",
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
            field: "resolve_elapsed"
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
}