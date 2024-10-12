const mysql = require('mysql2');
const amqp = require('amqplib');

var con = mysql.createPool({
    host: "localhost",
    user: "home_hydro",
    password: "plants2024",
    database: "home_hydro",
    connectionLimit: 10
});

con.getConnection(function(err, connection) {
    if (err) throw err;
    console.log("connected to mysql server");
    connection.release();
});

let humid_data = {
    zone1: "10",
    zone2: "11",
    zone3: "12",
    zone4: "13",
    zone5: "14",
    zone6: "15",
    zone7: "16",
    zone8: "17",
    serial: "123456",
    timestamp: new Date()
}

function insert_telemetry() {
    let sql = `INSERT INTO humidity_data (zone1, zone2, zone3, zone4, zone5, zone6, zone7, zone8, serial, timestamp) VALUES (?)`;
    var values = [
        humid_data.zone1, 
        humid_data.zone2, 
        humid_data.zone3,
        humid_data.zone4,
        humid_data.zone5,
        humid_data.zone6,
        humid_data.zone7,
        humid_data.zone8,
        humid_data.serial,
        humid_data.timestamp
    ];
    con.query(sql, [values], function(error, data) {
        if (error) throw error;
        console.log(data);
        return;
    });
};


insert_telemetry();