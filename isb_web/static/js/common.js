MISSING_VALUE = "-9999";

const FORMAT = "0,0";

function nFormat(v) {
    if (v === undefined) {
        return "";
    }
    if (v === MISSING_VALUE) {
        return v;
    }
    return numeral(v).format(FORMAT);
}

/*
Gather some basic info about the repo.
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

