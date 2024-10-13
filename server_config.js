/**
 * -------------------------------------------------------------------------
 * Project Name:       Home Hydroponics
 * File Name:          server_config.js
 * Description:        
 *                   
 * 
 * Author:             Daniel Yadov
 * Created On:         10/12/2024
 * Last Modified:      10/12/2024
 * Version:            1.0.0
 * Dependencies:       mysql2, amqplib, readline
 * -------------------------------------------------------------------------
 * Usage Instructions: 
 * -------------------------------------------------------------------------
 * Change Log:         1.0.0 - 10/12/2024 - Initial Release
 * -------------------------------------------------------------------------
 */

const mysql = require('mysql2');
const amqp = require('amqplib');
const readline = require('readline');

// MySQL connection pool setup
const connection = mysql.createPool({
    host: "localhost",
    user: "home_hydro",
    password: "plants2024",
    database: "home_hydro",
    connectionLimit: 10
});

// RabbitMQ connection URL
const rabbitMQUrl = 'amqp://admin:password@localhost'; // Replace with your RabbitMQ credentials
let channel;

// Create a readline interface for user input
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const database = "home_hydro";

// Function to connect to RabbitMQ
const connectToRabbitMQ = async () => {
    try {
        const connection = await amqp.connect(rabbitMQUrl);
        channel = await connection.createChannel();
        console.log("Connected to RabbitMQ");
    } catch (error) {
        console.error('Error connecting to RabbitMQ:', error);
        process.exit(1);
    }
};

// Check if a table exists
const checkTableExists = (tableName) => {
    return new Promise((resolve, reject) => {
        const query = `SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`;
        const params = [database, tableName];

        console.log('Checking table existence with parameters:', params);

        connection.execute(query, params, (err, results) => {
            if (err) {
                reject(err);
            } else {
                resolve(results[0].count > 0);
            }
        });
    });
};

// Function to normalize the data type
const normalizeDataType = (dataType) => {
    if (dataType.startsWith('varchar')) {
        return 'varchar';
    }
    return dataType;
};

// Verify the structure of the table
const verifyTableStructure = (tableName, expectedColumns) => {
    return new Promise((resolve, reject) => {
        const query = `SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE table_schema = ? AND table_name = ?`;
        const params = [database, tableName];

        console.log('Verifying table structure with parameters:', params);

        connection.execute(query, params, (err, results) => {
            if (err) {
                reject(err);
            } else {
                const actualColumns = results.map(row => ({
                    name: row.COLUMN_NAME,
                    type: normalizeDataType(row.DATA_TYPE)
                }));

                console.log('Actual columns:', actualColumns);

                const isValidStructure = expectedColumns.every(expectedCol => {
                    const matchingActualCol = actualColumns.find(actualCol =>
                        actualCol.name === expectedCol.name
                    );

                    if (matchingActualCol) {
                        return matchingActualCol.type === normalizeDataType(expectedCol.type);
                    } else {
                        return false;
                    }
                });

                resolve(isValidStructure);
            }
        });
    });
};

// Create a new table based on the table structure
const createTable = (tableName, columns) => {
    return new Promise((resolve, reject) => {
        const columnDefs = columns.map(col => `${col.name} ${col.type}`).join(', ');
        const query = `CREATE TABLE ${tableName} (${columnDefs})`;

        console.log('Creating table with query:', query);

        connection.execute(query, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve(`Table "${tableName}" created successfully.`);
            }
        });
    });
};

// Table structures definition
const table_structs = [
    {
        table_name: 'humidity_data',
        columns: [
            { name: 'zone1', type: 'varchar(255)' },
            { name: 'zone2', type: 'varchar(255)' },
            { name: 'zone3', type: 'varchar(255)' },
            { name: 'zone4', type: 'varchar(255)' },
            { name: 'zone5', type: 'varchar(255)' },
            { name: 'zone6', type: 'varchar(255)' },
            { name: 'zone7', type: 'varchar(255)' },
            { name: 'zone8', type: 'varchar(255)' },
            { name: 'serial', type: 'varchar(255)' },
            { name: 'timestamp', type: 'timestamp' }
        ]
    }
    // Add more table structures here if needed
];

// Function to check tables and create them if necessary
const checkAndCreateTables = async () => {
    for (const tableInfo of table_structs) {
        const tableName = tableInfo.table_name;
        const expectedColumns = tableInfo.columns;

        try {
            const tableExists = await checkTableExists(tableName);
            if (tableExists) {
                console.log(`Table "${tableName}" exists.`);
                const structureIsValid = await verifyTableStructure(tableName, expectedColumns);
                if (structureIsValid) {
                    console.log(`The structure of "${tableName}" is valid.`);
                } else {
                    console.log(`The structure of "${tableName}" is invalid.`);
                }
            } else {
                console.log(`Table "${tableName}" does not exist. Creating the table...`);
                const creationMessage = await createTable(tableName, expectedColumns);
                console.log(creationMessage);
            }
        } catch (err) {
            console.error(`Error checking or creating table "${tableName}":`, err);
        }
    }
};

// Function to send a message to RabbitMQ
const sendMessageToRabbitMQ = async (message) => {
    const queue = 'home_hydro'; // Define your queue name

    try {
        await channel.assertQueue(queue);
        channel.sendToQueue(queue, Buffer.from(message));
        console.log(`Sent message: "${message}" to queue: "${queue}"`);
    } catch (error) {
        console.error('Error sending message to RabbitMQ:', error);
    }
};

function generateRandomHumidityData() {
    const randomHumidity = (min, max) => (Math.random() * (max - min) + min).toFixed(1); // Generate random float between min and max

    const data = {
        type: 'humidity',
        zone1: parseFloat(randomHumidity(30, 80)), // Random value between 30 and 80
        zone2: parseFloat(randomHumidity(30, 80)),
        zone3: parseFloat(randomHumidity(30, 80)),
        zone4: parseFloat(randomHumidity(30, 80)),
        zone5: parseFloat(randomHumidity(30, 80)),
        zone6: parseFloat(randomHumidity(30, 80)),
        zone7: parseFloat(randomHumidity(30, 80)),
        zone8: parseFloat(randomHumidity(30, 80)),
        serial: generateSerial() // Generate a random serial number
    };

    return data;
}

function generateSerial() {
    const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    const numbers = '0123456789';
    const randomLetters = Array.from({ length: 3 }, () => letters.charAt(Math.floor(Math.random() * letters.length))).join('');
    const randomNumbers = Array.from({ length: 6 }, () => numbers.charAt(Math.floor(Math.random() * numbers.length))).join('');
    return randomLetters + randomNumbers; // Combine letters and numbers for the serial
}

const getCountFromTable = (tableName) => {
    return new Promise((resolve, reject) => {
        // Construct the query by inserting the table name directly into the string
        const query = `SELECT COUNT(*) AS count FROM \`${tableName}\``;

        console.log('Getting row count for table:', tableName);

        connection.execute(query, (err, results) => {
            if (err) {
                reject(err); // Reject with error if query fails
            } else {
                resolve(results[0].count); // Resolve with the count value
            }
        });
    });
};




// Menu for user interaction
const showMenu = () => {
    console.log("\nSelect an option:");
    console.log("1. Check database tables");
    console.log("2. Send a message to RabbitMQ");
    console.log("3. Generate 100 humidity packets");
    console.log("4. Exit");
    
    rl.question("Enter your choice (1-3): ", async (choice) => {
        switch (choice) {
            case '1':
                console.log("Checking database tables...");
                await checkAndCreateTables();
                showMenu(); // Show menu again after operation
                break;
            case '2':
                rl.question("Enter the message to send: ", async (message) => {
                    await sendMessageToRabbitMQ(message);
                    showMenu(); // Show menu again after operation
                });
                break;
            case '3':
                console.log("Begin Packet Spam");
                let pre_db_count = await getCountFromTable("humidity_data");
                for(let i = 0; i < 1000; i++) {
                    let data  = JSON.stringify(generateRandomHumidityData());
                    await sendMessageToRabbitMQ(data);
                }
                let db_count = await getCountFromTable("humidity_data");
                let loss = ((1 - (db_count - pre_db_count) / 1000)) * 100; 
                console.log(`Packet loss in DB: ${loss}`);
                break;
            case '4':
                console.log("Exiting...");
                connection.end();
                rl.close();
                return 0;
            default:
                console.log("Invalid choice. Please try again.");
                showMenu(); // Show menu again for invalid choice
        }
    });
};

// Start the application
const startApp = async () => {
    await connectToRabbitMQ(); // Connect to RabbitMQ
    showMenu(); // Show the menu for user interaction
};

// Execute the start function
startApp().catch((error) => {
    console.error('Error starting the application:', error);
});
