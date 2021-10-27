const MISSING_VALUE = "-9999";
const FORMAT = "0,0";
const HOVER_TIME = 500; //milliseconds
const SOLR_RESERVED = [' ', '+', '-', '&', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\\'];
const SOLR_VALUE_REGEXP = new RegExp("(\\" + SOLR_RESERVED.join("|\\") + ")", "g");
const DEFAULT_Q = "*:*";

// Used for keeping track of windows opened by app
var _windows = {};

/**
 * Escape a lucene / solr query term
 */
function escapeLucene(value) {
    return value.replace(SOLR_VALUE_REGEXP, "\\$1");
}

/**
 *  Format a number
 */
function nFormat(v) {
    if (v === undefined) {
        return "";
    }
    if (v === MISSING_VALUE) {
        return v;
    }
    return numeral(v).format(FORMAT);
}

/**
 * Gather some basic info about the repo.
 */
function siteInfo() {
    return {
        _info: "",
        init() {
            const url = "https://api.github.com/repos/isamplesorg/isamples_inabox/commits/develop";
            fetch(url)
                .then(response => response.json())
                .then(data => {
                    let _sha = data.sha.substr(0, 7);
                    var dmod = data.commit.author.date;
                    this._info = "Revision " + _sha + " at " + dmod;
                })
        }
    }
}

/**
 * Get the host of the page.
 *
 * This is useful for verifying messages are sent from windows on the same domain.
 */
function getPageHost() {
    return window.location.protocol + "//" + window.location.host;
}

/**
 * Get page URL query parameters.
 *
 * Example:
 *   _params = getPageQueryParams();
 *   query = _params.q || "*:*";
 */
function getPageQueryParams(){
    const _qry = new URLSearchParams(window.location.search);
    return Object.fromEntries(_qry.entries());
}


function openMapWindow() {
    if (_windows["map"] !== undefined) {
        if (!_windows["map"].closed) {
            return _windows["map"];
        }
    }
    const window_features = "menubar=yes,location=yes,resizable=yes,scrollbars=yes,status=yes";
    _windows["map"] = window.open("/map", "spatial", window_features);
}


function broadcastQuery(query, fquery) {
    const msg = {
        name: "set_query",
        q: query,
        fq: fquery
    }
    const _host = getPageHost();
    //window.postMessage(msg, _host);
    for (const [name, wndw] of Object.entries(_windows)) {
        console.log(`Post to ${name} = ${query}  ${fquery} by ${_host}`);
        wndw.postMessage(msg, "*");
    }
}
