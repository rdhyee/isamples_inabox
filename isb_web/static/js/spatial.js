/*
Javascript support for spatial.html
 */

function queryParams() {
    return {
        q: DEFAULT_Q,
        fq: "",
        update(ev){
            console.log(this.q);
            this.q = ev.detail.q || this.q;
            this.fq = ev.detail.fq;
            updateHeatmapLayer(this.q, this.fq);
        }
    }
}


function notifyQueryChanged(query, fquery) {
    let e = new CustomEvent("query-changed",{
        detail: {
            q:query,
            fq: fquery
        }
    });
    console.log(`notify: q:${query} fq:${fquery}`);
    window.dispatchEvent(e);
}

/*
Add an event listener to handle messages sent from controlling windows.
 */
window.addEventListener("message", (e) => {
    const _host = getPageHost();
    if (e.origin !== _host) {
        console.log(`Unhandled event from: ${e.origin}`);
        return;
    }
    if (e.data.name !== undefined) {
        if (e.data.name === "set_query") {
            notifyQueryChanged(e.data.q, e.data.fq);
            //document.getElementById("query").value = e.data.query;
        }
    }
}, false);
