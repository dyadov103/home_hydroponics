/**
 * -------------------------------------------------------------------------
 * Project Name:       Home Hydroponics
 * File Name:          message_processor.js
 * Description:        This program acts as a middleware for handling all
 *                     uplink and downlink packets between the sensors 
 *                     and the MySQL database using AMQP (RabbitMQ).
 * 
 *                     **Uplink Packets:** 
 *                     - Receives data packets from various sensors via AMQP.
 *                     - Processes and validates the incoming data.
 *                     - Stores the sensor data in the MySQL database, ensuring
 *                       data integrity and structure adherence.
 * 
 *                     **Downlink Packets:** 
 *                     - Listens for commands or control messages sent to the sensors.
 *                     - Processes these commands to adjust sensor configurations,
 *                       initiate readings, or perform other control functions.
 * 
 *                     This program serves as a crucial intermediary, 
 *                     ensuring seamless communication between the sensors
 *                     and the database while maintaining efficient data 
 *                     flow and control mechanisms.
 * 
 * Author:             Daniel Yadov
 * Created On:         10/12/2024
 * Last Modified:      10/12/2024
 * Version:            1.0.0
 * Dependencies:       mysql2, amqplib
 * -------------------------------------------------------------------------
 * Usage Instructions: This application can be hosted using either PM2 (preferred) or cron.
 * -------------------------------------------------------------------------
 * Change Log:         1.0.0 - 10/12/2024 - Initial Release
 * -------------------------------------------------------------------------
 */

const mysql = require('mysql2');
const amqp = require('amqplib');


//Supported packets
const supported_packets = [
    "humidity",
    "heartbeat",
    "water_ack"
]


const receiveMessage = async () => {
    try {
        // RabbitMQ connection URL with authentication
        const rabbitMQUrl = 'amqp://admin:password@localhost'; // Replace with your username, password, and host
        const connection = await amqp.connect(rabbitMQUrl);
        const channel = await connection.createChannel();
        const queue = 'home_hydro';

        // Assert (create) a queue if it doesn't already exist
        await channel.assertQueue(queue);

        console.log(`Subscribed to queue: "${queue}"`);

        // Consume messages from the queue
        channel.consume(queue, (msg) => {
            if (msg !== null) {
                const messageContent = msg.content.toString();
                console.log(`Received message: "${messageContent}"`);
                packet_router(messageContent);
                // Acknowledge the message
                channel.ack(msg);
            }
        });
    } catch (error) {
        console.error('Error receiving messages:', error);
    }
};

function packet_router(message) {
    try {
        let parsed = JSON.parse(message);
        if(supported_packets.includes(parsed.type)) {
            switch(parsed.type) {
                case supported_packets[0]:
                    insert_humid(parsed);
                    break;
                case supported_packets[1]:
                    insert_heartbeat(parsed);
                    break;
                case supported_packets[2]:
                    console.log("The plants have been watered!!");
                    break;
                default:
                    console.log("Unrecognized event type");
            }
        } 
        else {
            console.log(`Recieved and unsupported packet type "${parsed.type}"`);
        }
    }
    catch (error) {
        console.log(`JSON is likely malformed or contains unwanted escape characters. Stop trying to break me :(`);
        console.log(error);
    }
}




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



async function insert_humid(packet) {
    let sql = `INSERT INTO humidity_data (zone1, zone2, zone3, zone4, zone5, zone6, zone7, zone8, serial, timestamp) VALUES (?)`;
    var values = [
        packet.zone1, 
        packet.zone2, 
        packet.zone3,
        packet.zone4,
        packet.zone5,
        packet.zone6,
        packet.zone7,
        packet.zone8,
        packet.serial,
        new Date()
    ];
    try {
        const result = await con.promise().query(sql, [values]);
        console.log(`Saved humidity data into DB at ${new Date()}`);
        return result;
    } 
    catch (error) {
        console.log(`Error while Storing to DB`);
        console.error(error);
        return;
    }
};

async function insert_heartbeat(packet) {
    let sql = `INSERT INTO heartbeat_data (battery, dev_time, temperature, dev_humidity, serial, timestamp) VALUES (?)`;
    const values = [
        packet.battery,
        new Date(packet.dev_time),
        packet.temperature,
        packet.dev_humidity,
        packet.serial,
        new Date()
    ];

    try {
        const result = await con.promise().query(sql, [values]);
        console.log(`Saved heartbeat data into DB at ${new Date()}`);
        return result;
    } 
    catch (error) {
        console.log(`Error while Storing to DB`);
        console.error(error);
        return;
    }
}


// Call the function to start receiving messages
receiveMessage();