// Logging System
const systemglobal = require('./../config.json');
const colors = require('colors');
const sleep = (waitTimeInMs) => new Promise(resolve => setTimeout(resolve, waitTimeInMs));
const WebSocket = require('ws');
let logServerConn;
let logServerisConnected = false;
let unsentLogs = {};
let rollingIndex = 0;
let remoteLogger = false;
let flushTimeout;

function connectToWebSocket(serverUrl) {
    logServerConn = new WebSocket(serverUrl);

    logServerConn.onopen = () => {
        console.log('[LogServer] Connected to the server');
        logServerisConnected = true;
        clearTimeout(flushTimeout);
        flushTimeout = setTimeout(flushUnsentLogs, 5000);
    };
    logServerConn.onmessage = (event) => { handleIncomingMessage(event); };
    logServerConn.onclose = () => {
        //console.log('[LogServer] Disconnected from the server');
        logServerisConnected = false;
        reconnectToWebSocket(serverUrl);
    };
    logServerConn.onerror = (error) => {
        console.error('[LogServer] Error:', error);
        logServerisConnected = false;
        logServerConn.close();
    };
}
function reconnectToWebSocket(serverUrl) {
    //console.log('[LogServer] Attempting to reconnect...');
    setTimeout(() => {
        connectToWebSocket(serverUrl);
    }, 1000); // Reconnect attempt after 1 second
}
function handleIncomingMessage(event) {
    try {
        const data = JSON.parse(event.data);
        if (data.ack) {
            delete unsentLogs[data.id];
        }
    } catch (error) {
        console.error('[LogServer] Error parsing message:', error);
    }
}
function sendLog(proccess, text, level = 'debug', object, object2, color, no_ack = false) {
    const logId = generateLogId();
    const logEntry = {
        id: logId,
        message: text,
        level,
        time: new Date().valueOf(),
        server_name: systemglobal.SystemName,
        name: "MuginoMIITS",
        color,
        proccess,
        ack: !no_ack,
        extended: {
            object,
            object2
        }
    };
    if (!no_ack)
        unsentLogs[logId] = logEntry;
    if (logServerisConnected && logServerConn.readyState === WebSocket.OPEN)
        logServerConn.send(JSON.stringify(logEntry));
}
function flushUnsentLogs() {
    if (logServerisConnected && logServerConn.readyState === WebSocket.OPEN) {
        for (const logId in unsentLogs) {
            try {
                logServerConn.send(JSON.stringify(unsentLogs[logId]));
            } catch (error) {
                console.error(`[LogServer] Failed to send log ${logId}:`, error);
                break; // Stop flushing if sending fails
            }
        }
    }
}
function generateLogId() {
    // Increment rolling index and reset if it exceeds 9999
    rollingIndex = (rollingIndex + 1) % 10000;
    return `${Date.now()}-${rollingIndex}`;
}
if (systemglobal.log_server) {
    remoteLogger = true
    connectToWebSocket('ws://' + systemglobal.log_server);
    sendLog('Init', `Forwarding logs to Othinus Server`, 'debug');
    console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][Init] Forwarding logs to Othinus Server - MuginoMIITS`.gray);
}

async function printLine(proccess, text, level, object, object2, no_ack = false) {
    let logObject = {}
    let logClient = "Unknown"
    if (proccess) {
        logClient = proccess
    }
    logObject.process = logClient
    let logString =  `${text}`
    if (typeof object !== 'undefined' || (object && object !== null)) {
        if ( (typeof (object) === 'string' || typeof (object) === 'number' || object instanceof String) ) {
            logString += ` : ${object}`
        } else if (typeof(object) === 'object') {
            logObject = Object.assign({}, logObject, object)
            if (object.hasOwnProperty('message')) {
                logString += ` : ${object.message}`
            } else if (object.hasOwnProperty('sqlMessage')) {
                logString += ` : ${object.sqlMessage}`
            } else if (object.hasOwnProperty('itemFileData')) {
                delete logObject.itemFileData
                logObject.itemFileData = object.itemFileData.length
            } else if (object.hasOwnProperty('itemFileArray')) {
                delete logObject.itemFileArray
            }
        }
    }
    if (typeof object2 !== 'undefined' || (object2 && object2 !== null)) {
        if (typeof(object2) === 'string' || typeof(object2) === 'number' || object2 instanceof String) {
            logObject.extraMessage = object2.toString()
        } else if (typeof(object2) === 'object') {
            logObject = Object.assign({}, logObject, object2)
            if (object2.hasOwnProperty('itemFileData')) {
                delete logObject.itemFileData
                logObject.itemFileData = object2.itemFileData.length
            } else if (object.hasOwnProperty('itemFileArray')) {
                delete logObject.itemFileArray
            }
        }
    }
    if (level === "warn" || level === "warning") {
        if (remoteLogger)
            sendLog(logObject.process, logString, 'warning', logObject);
        console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.black.bgYellow)
        if (!text.toLowerCase().includes('block') && systemglobal.log_objects) {
            console.error(logObject)
        }
    } else if (level === "error" || level === "err") {
        if (remoteLogger)
            sendLog(logObject.process, logString, 'error', logObject);
        console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.black.bgRed)
        if (object)
            console.error(object)
        if (object2)
            console.error(object2)
    } else if (level === "critical" || level === "crit") {
        if (remoteLogger)
            sendLog(logObject.process, logString, 'critical', logObject);``
        console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.bgMagenta)
        if (object)
            console.error(object)
        if (object2)
            console.error(object2)
    } else if (level === "alert") {
        if (remoteLogger)
            sendLog(logObject.process, logString, 'alert', logObject);
        console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.red)
        console.log(logObject)
    } else if (level === "emergency") {
        if (remoteLogger)
            sendLog(logObject.process, logString, 'emergency', logObject);
        console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.bgMagenta)
        if (object)
            console.error(object)
        if (object2)
            console.error(object2)
        sleep(250).then(() => {
            process.exit(4);
        })
    } else if (level === "notice") {
        console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.green)
        if (remoteLogger)
            sendLog(logObject.process, logString, 'notice', logObject);
        if (systemglobal.log_objects) { console.log(logObject) }
    } else if (level === "debug") {
        if (text.includes('was approved!')) {
            if (remoteLogger)
                sendLog(logObject.process, logString, 'info', logObject, undefined, 'green');
            console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.black.bgGreen)
        } else if (text.includes('Blocked because')) {
            if (remoteLogger)
                sendLog(logObject.process, logString, 'info', logObject, undefined, 'red');
            console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.black.bgRed)
        } else {
            if (remoteLogger)
                sendLog(logObject.process, logString, 'debug', logObject);
            console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.gray)
        }
        if (systemglobal.log_objects) { console.log(logObject) }
    } else if (level === "info") {
        if (text.includes('was approved!')) {
            if (remoteLogger)
                sendLog(logObject.process, logString, 'info', logObject, undefined, 'green');
            console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.black.bgGreen)
        } else if (text.includes('Blocked because')) {
            if (remoteLogger)
                sendLog(logObject.process, logString, 'info', logObject, undefined, 'red');
            console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.black.bgRed)
        } else {
            if (remoteLogger)
                sendLog(logObject.process, logString, 'info', logObject);
            console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`.cyan.bgBlack)
        }
        if (systemglobal.log_objects) { console.log(logObject) }
    } else {
        if (remoteLogger)
            sendLog(logObject.process, logString, 'debug', logObject);
        if (systemglobal.log_objects) { console.log(logObject) }
        console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][${proccess}] ${text}`)
    }
}

process.on('uncaughtException', function(err) {
    console.log(err)
    console.log(`[${new Date().toLocaleDateString()} ${new Date().toLocaleTimeString()}][uncaughtException] ${err.message}`.bgRed);
    if (remoteLogger)
        sendLog('uncaughtException', `${err.message}`, 'critical');
});

module.exports = { printLine };
