/*
    File: server.js
    Purpose: create a local server to handle github login callback and send issues
*/



/*  EXPRESS */
const express = require('express');
const fetch = require('node-fetch');
const axios = require('axios')
const path = require('path');
const app = express();

const clientID = 'e1f28dcbaec464ecb538'
const clientSecret = '9ee28404b8aae760c389d94fd2702b48a6110625'

var access_token = "";

console.log(__dirname)

app.use('/img', express.static('img'))
app.use('/CSS', express.static('CSS'))
app.use('/js', express.static('js'))

app.get('/', function(req, res) {
    res.sendFile(path.join(__dirname, "/login.html"))
});

// Declare the callback route
app.get('/github/callback', (req, res) => {

    // The req.query object has the query params that were sent to this route.
    const requestToken = req.query.code

    axios({
        method: 'post',
        url: `https://github.com/login/oauth/access_token?client_id=${clientID}&client_secret=${clientSecret}&code=${requestToken}`,
        // Set the content type header, so that we get the response in JSON
        headers: {
            accept: 'application/json'
        }
    }).then((response) => {
        access_token = response.data.access_token
        res.redirect('/success');
    })
})

app.get('/success', function(req, res) {
    res.sendFile(path.join(__dirname, "/Records_View.html"))
});

app.get('/issues', function(req, res) {
    console.log(req.query)
    const payLoad = {
        title: req.query.title,
        body: req.query.report,
    }
    const headers = { "Authorization": "Token " + access_token };
    const url = `https://api.github.com/repos/qgan7125/testGithubApi/issues`;
    fetch(url, {
        method: "POST",
        headers: headers,
        body: JSON.stringify(payLoad)
    })

})




const port = process.env.PORT || 2400;
app.listen(port, () => console.log('App listening on port ' + port));