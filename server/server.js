const app= require('express')()
const port = 4000
const cors = require('cors')

var pgp = require('pg-promise')(/* options */)

/**
 * The URL to connect to POSTGRESQL database below has been replaced with a fake api due to security
 */
var db = pgp('URL_to_connect_to_postgreSQL_server')
app.use(cors())

app.get('/', (req, res) => {res.send('Hello World!')})

app.get('/:building_group/total/last_30_days', (req, res) => {

    db.many('SELECT * FROM building_group_last_30_days where building_group = $1', req.params.building_group)
    .then( (data) => {
        // data is a Javascript Array
        res.send(data)
    })
    .catch(function (error) {
        console.log('=== FAILED TO FETCH DATA FROM DATABASE === \n', error)
    })
})

app.get('/:building_group/total/last_30_days_last_year', (req, res) => {
    db.many('SELECT * FROM building_group_last_30_days_last_year where building_group = $1', req.params.building_group)
    .then(data => {
        res.send(data)
    })
    .catch(function (error) {
        console.log('=== FAILED TO FETCH DATA FROM DATABASE === \n', error)
    })
})

app.get('/:building_group/total/last_12_months', (req, res) => {
    db.many('SELECT * FROM building_group_last_12_months where building_group = $1', req.params.building_group)
    .then((data) => {
        // data is a Javascript Array
        res.send(data)
    })
    .catch(error => {
        console.log('=== FAILED TO FETCH DATA FROM DATABASE === \n', error)
    })
})

app.get('/:building_group/total/last_12_months_last_year', (req, res) => {
    db.many('SELECT * FROM building_group_last_12_months_last_year where building_group = $1', req.params.building_group)
    .then((data) => {
        // data is a Javascript Array
        res.send(data)
    })
    .catch(error => {
        console.log('=== FAILED TO FETCH DATA FROM DATABASE === \n', error)
    })
})

app.get('/:building_group/list', (req, res) => {
    db.many('select meter_name from meter_daily where building_group = $1 group by meter_name', req.params.building_group)
    .then((data) => {
        // data is a Javascript Array
        res.send(data)
    })
})


app.listen(port, () => console.log(`Listening on http://localhost:${port}`))