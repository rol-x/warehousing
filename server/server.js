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
    database: 'gathering',
    multipleStatements: true
});

var dateId = 1

/////////////////////////////////////////////////////

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function connect() {
    await sleep(10 * 1000);
    con.connect(async function (err) {
        if (err) { console.log(err.message); throw err; }
        console.log("Database connection established.");
        var check = "SELECT COUNT(DISTINCT `table_name`) AS tables \
                    FROM `information_schema`.`columns` \
                    WHERE `table_schema` = 'gathering'"
        wait = true;
        while (wait) {
            con.query(check, function (err, row) {
                if (err) throw err;
                wait = false;
                if (row[0].tables < 7) {
                    console.log("Waiting for the database to be ready...");
                    wait = true;
                }
            });
            if (wait) {
                await sleep(60 * 1000);
            }
        }
        var date = "SELECT MAX(date_id) AS this_date FROM sale_offer"
        con.query(date, function (err, row) {
            if (err) throw err;
            dateId = row[0].this_date;
            console.log("Date ID: " + dateId + ".");
        });
    });
} connect();

//////////////////////////////////////////////////////////

// Shows the cheapest reliable price of a card
async function cheapestBuy(req, res) {
    console.log("Cheapest Buy calculation (date_id: " + dateId + ")");

    var query = "DROP TABLE IF EXISTS V1"
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper table dropped.");
        }
    });
    await sleep(1000);

    var isFoiled = req.body.foil == true ? 1 : 0;
    var query = "CREATE TABLE V1 AS \
                 SELECT \
                        seller_id, \
                        card_id, \
                        date_id, \
                        AVG(price) as price, \
                        AVG(weekly_avg) AS weekly_average \
                 FROM last_two_weeks \
                 WHERE card_id IN (" + String(req.body.cards.join(", ")) + ") \
                   AND card_condition=\"" + String(req.body.cond) + "\" \
                   AND card_language=\"" + String(req.body.lang) + "\" \
                   AND is_foiled=" + isFoiled + " \
                 GROUP BY seller_id, card_id, date_id;"

    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper table created.");
        }
    });
    await sleep(1000);

    var query = "SELECT seller.name AS seller_name, \
                        V1.seller_id AS seller_id, \
                        seller.country AS seller_country, \
                        seller.address AS seller_address, \
                        AVG(price) AS avgerage_price, \
                        MIN(price) AS best_price \
                 FROM V1 \
                 LEFT JOIN seller ON V1.seller_id = seller.id \
                 GROUP BY V1.seller_id \
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

// Returns a seller with most of the provided cards
async function bestSetSeller(req, res) {
    console.log("Best Set Seller calculation (date_id: " + dateId + ")");

    var query = "DROP TABLE IF EXISTS V2; DROP TABLE IF EXISTS V3; DROP TABLE IF EXISTS V4";
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper tables dropped.");
        }
    });
    await sleep(1000);

    var isFoiled = req.body.foil == true ? 1 : 0;
    var query = "CREATE TABLE V2 \
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
            console.log("Helper table created.");
        }
    });
    await sleep(1000);

    var query = "CREATE TABLE V3 AS \
                    SELECT seller_id, COUNT(DISTINCT card_id) AS amount \
                 FROM V2 \
                 GROUP BY seller_id"
    con.query(query, (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        } else {
            console.log("Helper table created.");
        }
    });
    await sleep(1000);

    var query = "CREATE TABLE V4 AS \
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
            console.log("Helper table created.");
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

// Finds the best sellers for buying discounted cards
async function dealmakers(req, res) {
    console.log("Dealmakers calculation (date_id: " + dateId + ")");

    var query = "DROP TABLE IF EXISTS V5; DROP TABLE IF EXISTS V6; DROP TABLE IF EXISTS V7; \
                 CREATE TABLE V5 AS \
                 SELECT seller_id, COUNT(DISTINCT card_id) AS cards_amount FROM offers_today \
                 WHERE card_condition=? AND card_language=? AND is_foiled=? \
                 GROUP BY seller_id \
                 ORDER BY cards_amount DESC \
                 LIMIT 12; \
                 \
                 CREATE TABLE V6 AS \
                 SELECT card_id, AVG(price) AS avg_price \
                 FROM V5 LEFT JOIN offers_today off ON V5.seller_id=off.seller_id \
                 WHERE card_condition=? AND card_language=? AND is_foiled=? \
                 GROUP BY card_id; \
                 \
                 CREATE TABLE V7 AS \
                 SELECT V5.seller_id, SUM((avg_price-price)/avg_price) AS discount_index \
                 FROM V5 LEFT JOIN offers_today off ON V5.seller_id=off.seller_id \
                 JOIN V6 ON off.card_id=V6.card_id \
                 WHERE card_condition=? AND card_language=? AND is_foiled=? \
                 GROUP BY V5.seller_id \
                 HAVING discount_index>=0;"

    var isFoiled = req.body.foil == true ? 1 : 0;
    var params = [req.body.cond, req.body.lang, isFoiled,
    req.body.cond, req.body.lang, isFoiled,
    req.body.cond, req.body.lang, isFoiled]
    con.query(query, params, (err, row) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            console.log(err.message);
            return;
        }
        console.log("Helper tables created");
    });
    await sleep(2000);

    var query = "SELECT V7.seller_id AS id, name, country, type, address \
                 FROM V7 JOIN seller s ON V7.seller_id=s.id \
                 WHERE discount_index>=(SELECT AVG(discount_index) FROM V7)"
    con.query(query, (err, row) => {
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
                    "type": row[0].type,
                    "address": row[0].address,
                    "webpage": "https://www.cardmarket.com/en/Magic/Users/" + row[0].name + "/"
                });
            } else {
                res.json({
                    "id": row[0].id,
                    "name": row[0].name,
                    "country": row[0].country,
                    "type": row[0].type,
                    "webpage": "https://www.cardmarket.com/en/Magic/Users/" + row[0].name + "/"
                });
            }
        }
    });
}

// Orders provided cards by the discount available
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

app.post('/api/cheapest-buy', [cheapestBuy]);
app.post('/api/best-set-seller', [bestSetSeller]);
app.post('/api/dealmakers', [dealmakers]);
app.post('/api/deal-finder', [dealFinder]);
