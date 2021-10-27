    //change display context according to tabs
    function openTab(pid) {
        var data = document.getElementById("data").children;
        for (var i = 0; i < data.length; i++) {
            data[i].style.display = "none";
        }
        var actContent = document.getElementById(pid);
        actContent.style.display = "block";
    }
    //show specified identifier record
    async function showRawRecord(id) {
        var url = `https://mars.cyverse.org/thing/${id}?full=false`;
        console.log(url);
        fetch(url)
            .then(response => response.json())
            .then(doc => {
                var e = document.getElementById("object_show");
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