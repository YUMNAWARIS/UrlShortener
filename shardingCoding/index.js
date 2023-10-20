const app = require('express')()
const { Client } = require('pg')
const crypto = require("crypto")
const HashRing = require('hashring')
const cors = require('cors')

const hashRing = new HashRing()
hashRing.add("5432")
hashRing.add("5433")
hashRing.add("5434")

const clients = {
    5432: new Client({
        host: 'localhost',
        port: '5432',
        user: 'postgres',
        password: 'postgres',
        database: 'postgres'
    }),
    5433: new Client({
        host: 'localhost',
        port: '5433',
        user: 'postgres',
        password: 'postgres',
        database: 'postgres'
    }),
    5434: new Client({
        host: 'localhost',
        port: '5434',
        user: 'postgres',
        password: 'postgres',
        database: 'postgres'
    }),
}

const connect = async() => {
    await clients['5432'].connect()
    await clients['5433'].connect()
    await clients['5434'].connect()
}

connect()

app.use(cors())
app.get('/:urlId', async (req,res) => {
    const urlId = req.params.urlId
    const server = hashRing.get(urlId)
    const results = await clients[server].query('SELECT * FROM URL_TABLE WHERE URL_ID=$1', [urlId])
    if(results.rowCount > 0){
        res.send({
            url: results.rows[0].url,
            url_id: urlId,
            server
        })
    }else{
        res.send('Not Found')
    }
})

app.post('/', async (req,res) => {
    const url = req.query.url

    // consistent hash url
    const hash = crypto.createHash('sha256').update(url).digest('base64')
    const urlId = hash.substring(0, 5)
    const server = hashRing.get(urlId)
    
    await clients[server].query('INSERT INTO URL_TABLE (URL,URL_ID) VALUES($1,$2)', [url, urlId])
    res.send({
        url,
        url_id: urlId,
        server
    })
})

app.listen('8081', () => {
    console.log('running on 8081');
})