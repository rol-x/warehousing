var express = require('express');
var mysql = require('mysql2');
var cors = require('cors');

var app = express();
var PORT = process.env.PORT || 8080;

var server = app.listen(PORT, () => console.log(`Listening on ${PORT}`));
app.use(express.json());
app.use(cors());

app.use(express.static(__dirname + '/public'));

var con = mysql.createConnection({
    user: 'root',
    password: 'root',
    host: 'mysql_database',
    database: 'gathering'
});

var dateId = 1
con.connect(function (err) {
    if (err) { console.log(err.message); throw err; }
    var date = "SELECT MAX(date_id) AS this_date FROM sale_offer"
    con.query(date, function (err, row) {
        if (err) throw err;
        dateId = row[0].this_date;
        console.log("Connected to the database (Date ID: " + dateId + ").");
    });
});

//////////////////////////////////////////////////////////

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function cheapestBuy(req, res) {
    console.log("Cheapest Buy calculation (date_id: " + dateId + ")");

    var query = "DROP VIEW IF EXISTS V1"
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper view dropped.");
        }
    });
    await sleep(1000);

    var isFoiled = req.body.foil == true ? 1 : 0;
    var query = "CREATE VIEW T1 AS \
                 SELECT \
                        seller_id, \
                        card_id, \
                        date_id, \
                        AVG(price) as price, \
                        AVG(weekly_avg) AS weekly_average \
                 FROM V1 \
                 WHERE card_id IN (" + String(req.body.cards.join(", ")) + ") \
                   AND card_condition=\"" + String(req.body.cond) + "\" \
                   AND card_language=\"" + String(req.body.lang) + "\" \
                   AND is_foiled=" + isFoiled + " \
                 GROUP BY seller_id, cs.card_id, cs.date_id;"

    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper view created.");
        }
    });
    await sleep(1000);

    var query = "SELECT seller.name AS seller_name, \
                        T1.seller_id AS seller_id, \
                        seller.country AS seller_country, \
                        seller.address AS seller_address, \
                        AVG(price) AS avgerage_price, \
                        MIN(price) AS best_price \
                 FROM T1 \
                 LEFT JOIN seller ON V1.seller_id = seller.id \
                 GROUP BY T1.seller_id \
                 HAVING avgerage_price <= AVG(weekly_average) \
                 ORDER BY best_price \
                 LIMIT 3"
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log(rows);
            res.json({
                "data": rows,
            });
        }
    });
}

async function bulkBuy(req, res) {
    console.log("Bulk Buy calculation (date_id: " + dateId + ")");

    var seller_id = 0
    var query = "SELECT seller_id \
                 FROM sale_offer \
                 WHERE date_id=? AND price<? \
                 AND card_condition=? AND card_language=? AND is_foiled=? \
                 GROUP BY seller_id \
                 ORDER BY COUNT(DISTINCT card_id) DESC \
                 LIMIT 1;"

    var isFoiled = req.body.foil == true ? 1 : 0;
    var params = [dateId, req.body.money, req.body.cond, req.body.lang, isFoiled]
    console.log(params)
    con.query(query, params, (err, row) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else if (row != undefined) {
            seller_id = row[0].seller_id;
        }
    });
    await sleep(2000);
    if (seller_id == 0) {
        res.json({ "response": "No results found." });
        return;
    }
    console.log("Wanted seller_id is " + seller_id)

    var query = "SELECT id, name, country, address FROM seller WHERE id=?"
    var params = [seller_id]
    con.query(query, params, (err, row) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            if (row[0].address != "") {
                res.json({
                    "id": row[0].id,
                    "name": row[0].name,
                    "country": row[0].country,
                    "address": row[0].address,
                    "webpage": "https://www.cardmarket.com/en/Magic/Users/" + row[0].name + "/"
                });
            } else {
                res.json({
                    "id": row[0].id,
                    "name": row[0].name,
                    "country": row[0].country,
                    "webpage": "https://www.cardmarket.com/en/Magic/Users/" + row[0].name + "/"
                });
            }
        }
    });
}

async function bestSetSeller(req, res) {
    console.log("Best Set Seller calculation (date_id: " + dateId + ")");

    var query = "DROP VIEW IF EXISTS V2";
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper view dropped.");
        }
    });
    var query = "DROP VIEW IF EXISTS V3";
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper view dropped.");
        }
    });
    var query = "DROP VIEW IF EXISTS V4";
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper view dropped.");
        }
    });
    await sleep(1000);

    var isFoiled = req.body.foil == true ? 1 : 0;
    var query = "CREATE VIEW V2 \
                 AS \
                    SELECT seller_id, card_id, MIN(price) AS best_price \
                 FROM sale_offer \
                 WHERE date_id=" + String(dateId) + " \
                    AND card_id IN (" + String(req.body.cards.join(", ")) + ") \
                    AND card_condition=\"" + String(req.body.cond) + "\" \
                    AND card_language=\"" + String(req.body.lang) + "\" \
                    AND is_foiled=" + isFoiled + " \
                 GROUP BY seller_id, card_id"
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper view created.");
        }
    });
    await sleep(1000);

    var query = "CREATE VIEW V3 AS \
                    SELECT seller_id, COUNT(DISTINCT card_id) AS amount \
                 FROM V2 \
                 GROUP BY seller_id"
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper view created.");
        }
    });
    await sleep(1000);

    var query = "CREATE VIEW V4 AS \
                 SELECT seller_id, SUM(best_price) AS total_price \
                 FROM V2 \
                 WHERE seller_id IN ( \
                    SELECT seller_id \
                    FROM V3 \
                    WHERE amount=(SELECT MAX(amount) FROM V3)) \
                 GROUP BY seller_id \
                 ORDER BY total_price LIMIT 3"
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper view created.");
        }
    });
    await sleep(1000);

    var query = "SELECT V4.seller_id, \
                        seller.name, \
                        seller.country, \
                        seller.address, \
                        V3.amount, \
                        V4.total_price \
                 FROM V4 \
                 LEFT JOIN seller ON V4.seller_id=seller.id \
                 LEFT JOIN V3 ON V4.seller_id=V3.seller_id"
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log(rows);
            res.json({
                "data": rows,
            });
        }
    });
}


function dealFinder(req, res) {
    var isFoiled = req.body.foil == true ? 1 : 0;
    var query = "SELECT card_id, \
                        MIN(price) AS best_price, \
                        AVG(price) AS average_price, \
                        (AVG(price)-MIN(price))/AVG(price) as discount \
                 FROM sale_offer \
                 WHERE date_id=" + String(dateId) + " \
                    AND card_id IN (" + String(req.body.cards.join(", ")) + ") \
                    AND card_condition=\"" + String(req.body.cond) + "\" \
                    AND card_language=\"" + String(req.body.lang) + "\" \
                    AND is_foiled=" + isFoiled + " \
                GROUP BY card_id \
                ORDER BY discount DESC"
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log(rows);
            res.json({
                "data": rows
            })
        }
    });
}

//////////////////////////////////////////////////////////

app.post('/api/bulk-buy', [bulkBuy]);
app.post('/api/cheapest-buy', [cheapestBuy]);
app.post('/api/best-set-seller', [bestSetSeller]);
app.post('/api/deal-finder', [dealFinder]);
