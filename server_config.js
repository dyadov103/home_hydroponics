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

// Menu for user interaction
const showMenu = () => {
    console.log("\nSelect an option:");
    console.log("1. Check database tables");
    console.log("2. Send a message to RabbitMQ");
    console.log("3. Exit");
    
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
