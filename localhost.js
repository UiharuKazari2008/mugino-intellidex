(async () => {
    let systemglobal = require('./config.json');
    if (process.env.SYSTEM_NAME && process.env.SYSTEM_NAME.trim().length > 0)
        systemglobal.system_name = process.env.SYSTEM_NAME.trim()
    const facilityName = 'MuginoMIITS';
    const sleep = (waitTimeInMs) => new Promise(resolve => setTimeout(resolve, waitTimeInMs));
    const { spawn, exec } = require("child_process");
    const fs = require('fs');
    const path = require('path');
    const chokidar = require('chokidar');
    const fileType = require('detect-file-type');
    const sharp = require('sharp');
    const splitFile = require('split-file');
    const rimraf = require('rimraf');
    const storageHandler = require('node-persist');
    const request = require('request').defaults({ encoding: null, jar: true });
    const { sqlPromiseSafe } = require("./utils/sqlClient");
    const Logger = require('./utils/logSystem');
    const { DiscordSnowflake } = require('@sapphire/snowflake');
    const crypto = require('crypto');
    const express = require('express');
    const cron = require('node-cron');
    const moment = require('moment');
    const jpeg = require('jpeg-js');
    const os = require('os');
    const globalRunKey = crypto.randomBytes(5).toString("hex");
    let amqpConn = null;
    const Discord_CDN_Accepted_Files = ['jpg','jpeg','jfif','png','webp','gif'];
    let active = true;
    let pastJobs = [];
    let parsedImages = [];
    let warnedImages = [];
    let totalItems = 0;
    const bootTime = Date.now();
    const LOG_FILE_PATH = path.join(__dirname, 'logs.json');
    const MAX_LOG_ENTRIES = 10000;

    function formatLogMessage(level, message) {
        // If the message is an object, convert it to stringified JSON
        if (typeof message === 'object') {
            message = JSON.stringify(message);
        }

        // If it's an error object with a "message" property, extract it
        if (level === 'error' && typeof message === 'object' && message.message) {
            message = message.message;
        }

        return message;
    }
    // Custom logger function
    function customLogger(level, message) {
        const timestamp = new Date();
        const formattedMessage = formatLogMessage(level, message);
        const logEntry = { error: level === 'error', message: formattedMessage, time: timestamp };

        // Save to console
        console[level](message);

        // Save to file
        const logs = JSON.parse(fs.readFileSync(LOG_FILE_PATH, 'utf-8'));
        logs.push(logEntry);

        // Keep only the last 10,000 entries
        if (logs.length > MAX_LOG_ENTRIES) {
            logs.splice(0, logs.length - MAX_LOG_ENTRIES);
        }

        fs.writeFileSync(LOG_FILE_PATH, JSON.stringify(logs, null, 2));
    }

    const app = express();

    // Schedule a cron job to clean up logs every hour
    cron.schedule('0 * * * *', () => {
        const logs = JSON.parse(fs.readFileSync(LOG_FILE_PATH, 'utf-8'));
        if (logs.length > MAX_LOG_ENTRIES) {
            const updatedLogs = logs.slice(-MAX_LOG_ENTRIES);
            fs.writeFileSync(LOG_FILE_PATH, JSON.stringify(updatedLogs, null, 2));
            customLogger('log', 'Log file cleaned up to keep the last 10,000 entries.');
        }
    });
    // Express route to view logs
    app.get('/log', (req, res) => {
        const logs = JSON.parse(fs.readFileSync(LOG_FILE_PATH, 'utf-8'));

        const logTable = logs
            .reverse()
            .map((log) => {
                const timeFromNow = moment(log.time).fromNow();
                const color = log.error ? 'red' : 'black';
                return `
        <tr style="color: ${color}">
          <td>${timeFromNow}</td>
          <td>${log.message}</td>
        </tr>
      `;
            })
            .join('');

        res.send(`
    <html>
      <head>
        <title>Mugino MIITS Log Viewer</title>
        <style>
          table { width: 100%; border-collapse: collapse; }
          th, td { padding: 8px; border: 1px solid #ddd; }
          th { background-color: #f4f4f4; }
        </style>
      </head>
      <body>
        <h1>Mugino Log Viewer</h1>
        <h4>Uptime: ${((Date.now() - bootTime) / 60000).toFixed(2)} // Total Parsed: ${totalItems}</h4>
        <table>
          <thead>
            <tr>
              <th>Time from Now</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody>
            ${logTable}
          </tbody>
        </table>
      </body>
    </html>
  `);
    });

    // Initialize log file if not exists
    if (!fs.existsSync(LOG_FILE_PATH)) {
        fs.writeFileSync(LOG_FILE_PATH, '[]');
    }

    if (fs.existsSync(path.join('./', 'watchedFiles.json'))) {
        try {
            const json = JSON.parse(fs.readFileSync(path.join('./', 'watchedFiles.json')).toString());
            if (json.catchList)
                warnedImages = json.catchList;
        } catch (e) {
            customLogger('error', "Error Reading Catch List: ", e.message)
        }
    }

    app.use(express.static(path.join('./utils/models/')));

    async function loadDatabaseCache() {
        Logger.printLine("SQL", "Getting System Parameters", "debug")
        const _systemparams = await sqlPromiseSafe(`SELECT * FROM global_parameters WHERE (system_name = ? OR system_name IS NULL) AND (application = 'mugino' OR application IS NULL) ORDER BY system_name, application, account`, [systemglobal.system_name])
        if (_systemparams.error) { Logger.printLine("SQL", "Error getting system parameter records!", "emergency", _systemparams.error); return false }
        const systemparams_sql = _systemparams.rows.reverse();

        if (systemparams_sql.length > 0) {
            const _mq_account = systemparams_sql.filter(e => e.param_key === 'mq.login');
            if (_mq_account.length > 0 && _mq_account[0].param_data) {
                if (_mq_account[0].param_data.host)
                    systemglobal.mq_host = _mq_account[0].param_data.host;
                if (_mq_account[0].param_data.username)
                    systemglobal.mq_user = _mq_account[0].param_data.username;
                if (_mq_account[0].param_data.password)
                    systemglobal.mq_pass = _mq_account[0].param_data.password;
            }
            // MQ Login - Required
            // MQServer = "192.168.250.X"
            // MQUsername = "eiga"
            // MQPassword = ""
            // mq.login = { "host" : "192.168.250.X", "username" : "eiga", "password" : "" }
            const _watchdog_host = systemparams_sql.filter(e => e.param_key === 'watchdog.host');
            if (_watchdog_host.length > 0 && _watchdog_host[0].param_value) {
                systemglobal.Watchdog_Host = _watchdog_host[0].param_value;
            }
            // Watchdog Check-in Hostname:Port or IP:Port
            // Watchdog_Host = "192.168.100.X"
            // watchdog.host = "192.168.100.X"
            const _watchdog_id = systemparams_sql.filter(e => e.param_key === 'watchdog.id');
            if (_watchdog_id.length > 0 && _watchdog_id[0].param_value) {
                systemglobal.Watchdog_ID = _watchdog_id[0].param_value;
            }
            // Watchdog Check-in Group ID
            // Watchdog_ID = "main"
            // watchdog.id = "main"
            const _cluster_id = systemparams_sql.filter(e => e.param_key === 'cluster.id');
            if (_cluster_id.length > 0 && _cluster_id[0].param_value) {
                systemglobal.Cluster_ID = _cluster_id[0].param_value;
            }
            const _cluster_entity = systemparams_sql.filter(e => e.param_key === 'cluster.entity');
            if (_cluster_entity.length > 0 && _cluster_entity[0].param_value) {
                systemglobal.Cluster_Entity = _cluster_entity[0].param_value;
            }
            const _cluster_fail = systemparams_sql.filter(e => e.param_key === 'cluster.fail_time');
            if (_cluster_fail.length > 0 && _cluster_fail[0].param_value) {
                systemglobal.Cluster_Comm_Loss_Time = parseFloat(_cluster_fail[0].param_value.toString());
            }

            const _mq_discord_out = systemparams_sql.filter(e => e.param_key === 'mq.discord.out');
            if (_mq_discord_out.length > 0 && _mq_discord_out[0].param_value) {
                systemglobal.mq_discord_out = _mq_discord_out[0].param_value;
            }
            // Discord Outbox MQ - Required - Dynamic
            // Discord_Out = "outbox.discord"
            // mq.discord.out = "outbox.discord"
            const _mq_pdp_in = systemparams_sql.filter(e => e.param_key === 'mq.pdp.out');
            if (_mq_pdp_in.length > 0 && _mq_pdp_in[0].param_value) {
                systemglobal.mq_mugino_in = _mq_pdp_in[0].param_value;
            }
            const _mq_pdp_bulk = systemparams_sql.filter(e => e.param_key === 'mq.pdp.bulk');
            if (_mq_pdp_bulk.length > 0 && _mq_pdp_bulk[0].param_value) {
                systemglobal.mq_mugino_in_bulk = _mq_pdp_bulk[0].param_value;
            }
            // Mugino Inbox MQ - Dynamic
            // Mugino_In = "inbox.mugino"
            // mq.pdp.out = "inbox.mugino"
            const _mugino_config = systemparams_sql.filter(e => e.param_key === 'mugino.config');
            if (_mugino_config.length > 0 && _mugino_config[0].param_data) {
                if (_mugino_config[0].param_data.search)
                    systemglobal.search = _mugino_config[0].param_data.search;
                if (_mugino_config[0].param_data.rules)
                    systemglobal.rules = _mugino_config[0].param_data.rules;
                if (_mugino_config[0].param_data.pull_limit)
                    systemglobal.pull_limit = _mugino_config[0].param_data.pull_limit;
                if (_mugino_config[0].param_data.parallel_downloads)
                    systemglobal.parallel_downloads = _mugino_config[0].param_data.parallel_downloads;
            }
            // Mugino Config

        }

        Logger.printLine("SQL", "All SQL Configuration records have been assembled!", "debug");
        setTimeout(loadDatabaseCache, 1200000)
    }
    await loadDatabaseCache();
    console.log(systemglobal);

    function checkMemoryUsage() {
        const totalMemory = os.totalmem(); // Total memory in bytes
        const freeMemory = os.freemem(); // Free memory in bytes
        const usedMemory = totalMemory - freeMemory;
        const usedMemoryPercentage = (usedMemory / totalMemory) * 100;

        customLogger('log', `Total Memory: ${(totalMemory / (1024 * 1024)).toFixed(2)} MB`);
        customLogger('log', `Used Memory: ${(usedMemory / (1024 * 1024)).toFixed(2)} MB`);
        customLogger('log', `Used Memory Percentage: ${usedMemoryPercentage.toFixed(2)}%`);

        if (usedMemoryPercentage > 95 && systemglobal.reset_on_overload) {
            customLogger('error', "Memory overflow prevention");
            process.exit(1);
        }
    }

    const mqClient = require('./utils/mqAccess')(facilityName, systemglobal);

    customLogger('log', "Reading tags from database...");
    let exsitingTags = new Map();
    (await sqlPromiseSafe(`SELECT id, name FROM sequenzia_index_tags`)).rows.map(e => exsitingTags.set(e.name, e.id));
    customLogger('log', "Reading tags from model...");
    let modelTags = new Map();
    const _modelTags = (fs.readFileSync(path.join(systemglobal.deepbooru_model_path, './tags.txt'))).toString().trim().split('\n').map(line => line.trim());
    const _modelCategories = JSON.parse(fs.readFileSync(path.join(systemglobal.deepbooru_model_path, './categories.json')).toString());
    Object.values(_modelCategories).map((e,i,a) => {
        const c = ((n) => {
            switch (n) {
                case 'General':
                    return 1;
                case 'Character':
                    return 2;
                case 'System':
                    return 3;
                default:
                    return 0;
            }
        })(e.name);
        _modelTags.slice(e.start_index, ((i+1 !== a.length) ? a[i+1].start_index - 1 : undefined)).map(t => {
            modelTags.set(t, c)
        })
    })
    customLogger('log', `Loaded ${modelTags.size} tags from model`);
    const activeFiles = new Map();
    let init = false;


    Logger.printLine("Init", "Mugino Orchestrator Server", "debug");
    const baseKeyName = `mugino.${systemglobal.system_name}.`

    const LocalQueue = storageHandler.create({
        dir: 'data/LocalQueue',
        stringify: JSON.stringify,
        parse: JSON.parse,
        encoding: 'utf8',
        logging: false,
        ttl: false,
        expiredInterval: 2 * 60 * 1000, // every 2 minutes the process will clean-up the expired cache
        forgiveParseErrors: true
    });
    LocalQueue.init((err) => {
        if (err) {
            Logger.printLine("LocalQueue", "Failed to initialize the local request storage", "error", err)
        } else {
            Logger.printLine("LocalQueue", "Initialized successfully the local request storage", "debug", err)
        }
    });
    const UpscaleQueue = storageHandler.create({
        dir: 'data/UpscaleQueue',
        stringify: JSON.stringify,
        parse: JSON.parse,
        encoding: 'utf8',
        logging: false,
        ttl: false,
        expiredInterval: 2 * 60 * 1000, // every 2 minutes the process will clean-up the expired cache
        forgiveParseErrors: true
    });
    UpscaleQueue.init((err) => {
        if (err) {
            Logger.printLine("UpscaleQueue", "Failed to initialize the upscale request storage", "error", err)
        } else {
            Logger.printLine("UpscaleQueue", "Initialized successfully the upscale request storage", "debug", err)
        }
    });

    const ruleSets = new Map();

    let startEvaluating = null;
    let startUpscaleing = null;
    let gpuLocked = false;
    let upscaleIsActive = false;
    let mittsIsActive = false;
    let runTimer = null;
    let shutdownRequested = false;
    let model
    let watchEnabled = false;
    async function watchResults() {
        if (!watchEnabled) {
            watchEnabled = true;
            const resultsWatcher = chokidar.watch(systemglobal.deepbooru_output_path, {
                ignored: /[\/\\]\./,
                persistent: true,
                usePolling: true,
                awaitWriteFinish: {
                    stabilityThreshold: 2000,
                    pollInterval: 100
                },
                depth: 1,
                ignoreInitial: false
            });
            resultsWatcher
                .on('add', async function (filePath) {
                    if (warnedImages[path.basename(filePath)] !== undefined)
                        delete warnedImages[path.basename(filePath)];
                    if (filePath.split('/').pop().split('\\').pop().endsWith('.json') && filePath.split('/').pop().split('\\').pop().startsWith('query-')) {
                        const eid = path.basename(filePath).split('query-').pop().split('.')[0];
                        const message = (await sqlPromiseSafe(`SELECT * FROM kanmi_records WHERE eid = ?`, [eid])).rows;
                        const jsonFilePath = path.resolve(filePath);
                        const imageFile = fs.readdirSync(systemglobal.deepbooru_input_path)
                            .filter(k => k.split('.')[0] === path.basename(filePath).split('.')[0]).pop();
                        const tagResults = JSON.parse(fs.readFileSync(jsonFilePath).toString());
                        let extra = '';
                        if (message.length > 0) {
                            const rs = await parseResultsForQuery(message[0].channel, tagResults)
                            if (!rs.approval) {
                                extra += ', hidden = 1'
                                customLogger('log', `Entity ${eid} will be hidden!`);
                            } else {
                                customLogger('log', `Entity ${eid} was approved!`);
                            }
                            if (rs.folder)
                                extra += `, fid = ${rs.folder}`
                        }
                        let tagString = (Object.keys(tagResults).map(k => `${modelTags.get(k) || 0}/${parseFloat(tagResults[k]).toFixed(4)}/${k}`).join('; ') + '; ');
                        let safety = null;
                        customLogger('log', `Entity ${eid} has ${Object.keys(tagResults).length} tags!`);
                        await sqlPromiseSafe(`UPDATE kanmi_records SET tags = ?, safety = ?${extra} WHERE eid = ?`, [tagString, safety, eid])
                        Object.keys(tagResults).map(async k => {
                            const r = tagResults[k];
                            await addTagForEid(eid, k, r);
                        });
                        try {
                            fs.unlinkSync(jsonFilePath);
                        } catch (e) {

                        }
                        try {
                            if (imageFile)
                                fs.unlinkSync(path.join(systemglobal.deepbooru_input_path, (imageFile)));
                        } catch (e) {

                        }
                        activeFiles.delete(eid);
                    } else if (filePath.split('/').pop().split('\\').pop().endsWith('.json') && filePath.split('/').pop().split('\\').pop().startsWith('message-')) {
                        const key = path.basename(filePath).split('message-').pop().split('.')[0];
                        const jsonFilePath = path.resolve(filePath);
                        const imageFile = fs.readdirSync(systemglobal.deepbooru_input_path)
                            .filter(k => k.split('.')[0] === path.basename(filePath).split('.')[0]).pop();
                        const tagResults = JSON.parse(fs.readFileSync(jsonFilePath).toString());
                        const approved = await parseResultsForMessage(key, tagResults);
                        customLogger('error', `Message ${key} has ${Object.keys(tagResults).length} tags!`);
                        if (approved) {
                            mqClient.sendData(`${approved.destination}`, approved.message, function (ok) { });
                            customLogger('log', `Message ${key} was approved!`.green.bgBlack);
                        } else {
                            customLogger('error', `Message ${key} was denied! Will not be delivered!`.white.bgRed);
                        }
                        try {
                            fs.unlinkSync(jsonFilePath);
                        } catch (e) { }
                        try {
                            if (imageFile)
                                fs.unlinkSync(path.join(systemglobal.deepbooru_input_path, (imageFile)));
                        } catch (e) { }
                        await LocalQueue.removeItem(key);
                    } else if ((filePath.split('/').pop().split('\\').pop().endsWith('.jpg') || filePath.split('/').pop().split('\\').pop().endsWith('.png')) && filePath.split('/').pop().split('\\').pop().startsWith('upscale-')) {
                        const key = path.basename(filePath).split('upscale-').pop().split('.')[0];
                        customLogger('error', `Message ${key} has been upscaled!`);

                        mqClient.sendData(`${approved.destination}`, approved.message, function (ok) {
                        });
                        try {
                            fs.unlinkSync(filePath);
                        } catch (e) {

                        }
                        const imageFile = fs.readdirSync(systemglobal.waifu2x_input_path)
                            .filter(k => k.split('.')[0] === path.basename(filePath).split('.')[0]).pop();
                        try {
                            if (imageFile)
                                fs.unlinkSync(path.join(systemglobal.waifu2x_input_path, (imageFile)));
                        } catch (e) {

                        }
                        await UpscaleQueue.removeItem(key);
                    }
                })
                .on('error', function (error) {
                    customLogger('error', error);
                })
                .on('ready', function () {
                    customLogger('log', "MIITS Results Watcher Ready!")
                });
        }
    }

    if (systemglobal.Watchdog_Host && systemglobal.Watchdog_ID && !systemglobal.Cluster_ID) {
        request.get(`http://${systemglobal.Watchdog_Host}/watchdog/init?id=${systemglobal.Watchdog_ID}&entity=${facilityName}-${systemglobal.system_name}`, async (err, res) => {
            if (err || res && res.statusCode !== undefined && res.statusCode !== 200) {
                customLogger('error', `Failed to init watchdog server ${systemglobal.Watchdog_Host} as ${facilityName}:${systemglobal.Watchdog_ID}`);
            }
        })
        setInterval(() => {
            request.get(`http://${systemglobal.Watchdog_Host}/watchdog/ping?id=${systemglobal.Watchdog_ID}&entity=${facilityName}-${systemglobal.system_name}`, async (err, res) => {
                if (err || res && res.statusCode !== undefined && res.statusCode !== 200) {
                    customLogger('error', `Failed to ping watchdog server ${systemglobal.Watchdog_Host} as ${facilityName}:${systemglobal.Watchdog_ID}`);
                }
            })
        }, 60000)
    }
    let activeNode = false;
    let shutdownComplete = false;
    let lastClusterCheckin = (new Date().getTime());
    let checkinTimer = null;
    if (systemglobal.Watchdog_Host && systemglobal.Cluster_ID) {
        checkinTimer = setInterval(async () => {
            if (((new Date().getTime() - lastClusterCheckin) / 60000).toFixed(2) >= (systemglobal.Cluster_Comm_Loss_Time || 4.5)) {
                if (active || systemglobal.mq_mugino_in_bulk) {
                    Logger.printLine("ClusterIO", "Cluster Manager Communication was lost, No longer listening!", "critical");
                    shutdownRequested = true;
                    if (amqpConn)
                        amqpConn.close();
                    clearTimeout(startEvaluating);
                    startEvaluating = null;
                    if (!gpuLocked)
                        await processGPUWorkloads();
                    await waitForGPUUnlock();
                    shutdownComplete = true;
                    activeNode = false;
                }
            }
            request.get(`http://${systemglobal.Watchdog_Host}/cluster/ping?id=${systemglobal.Cluster_ID}&entity=${(systemglobal.Cluster_Entity) ? systemglobal.Cluster_Entity : facilityName + "-" + systemglobal.system_name}`, async (err, res, body) => {
                if (err || res && res.statusCode !== undefined && res.statusCode !== 200) {
                    customLogger('error', `Failed to ping watchdog server ${systemglobal.Watchdog_Host} as ${(systemglobal.Cluster_Entity) ? systemglobal.Cluster_Entity : facilityName + "-" + systemglobal.system_name}:${systemglobal.Cluster_ID}`);
                } else {
                    const jsonResponse = JSON.parse(Buffer.from(body).toString());
                    if (jsonResponse.error) {
                        customLogger('error', jsonResponse.error);
                    } else {
                        lastClusterCheckin = (new Date().getTime())
                        if (!jsonResponse.active) {
                            if (activeNode) {
                                Logger.printLine("ClusterIO", "System is not active, Shutting Down...", "warn");
                                shutdownRequested = true;
                                if (amqpConn)
                                    amqpConn.close();
                                clearTimeout(startEvaluating);
                                startEvaluating = null;
                                if (!gpuLocked)
                                    await processGPUWorkloads();
                                await waitForGPUUnlock();
                                shutdownComplete = true;
                                activeNode = false;
                                process.exit(1);
                            }
                        } else if (!activeNode) {
                            Logger.printLine("ClusterIO", "System is now active master, Rebooting...", "warn");
                            process.exit(1);
                        }
                    }
                }
            })
        }, 30000)
        app.get('/node_state', async (req, res) => {
            res.status(200).send(activeNode);
        })
        app.get('/state', async (req, res) => {
            res.status(200).send((activeNode) ? "Active" : (checkinTimer !== null) ? "Ready" : "Inactive");
        })
        await new Promise(async (cont) => {
            const isBootable = await new Promise(ok => {
                request.get(`http://${systemglobal.Watchdog_Host}/cluster/init?id=${systemglobal.Cluster_ID}&entity=${(systemglobal.Cluster_Entity) ? systemglobal.Cluster_Entity : facilityName + "-" + systemglobal.system_name}`, async (err, res, body) => {
                    if (err || res && res.statusCode !== undefined && res.statusCode !== 200) {
                        customLogger('error', `Failed to init watchdog server ${systemglobal.Watchdog_Host} as ${(systemglobal.Cluster_Entity) ? systemglobal.Cluster_Entity : facilityName + "-" + systemglobal.system_name}:${systemglobal.Cluster_ID}`);
                        ok(systemglobal.Cluster_Global_Master || false);
                    } else {
                        const jsonResponse = JSON.parse(Buffer.from(body).toString());
                        customLogger('log', jsonResponse);
                        if (jsonResponse.error) {
                            customLogger('error', jsonResponse.error);
                            ok(false);
                        } else {
                            if (!jsonResponse.active) {
                                Logger.printLine("ClusterIO", "System is not active, Standing by...", "warn");
                            }
                            ok(jsonResponse.active);
                        }
                    }
                })
            })
            if (!isBootable) {
                Logger.printLine("ClusterIO", "System is not active master", "warn");
                if (systemglobal.mq_mugino_in_bulk) {
                    Logger.printLine("KanmiMQ", "Node is processing in bulk mode", "warning");
                    const RateLimiter = require('limiter').RateLimiter;
                    const limiter = new RateLimiter(5, 5000);
                    const limiterlocal = new RateLimiter(1, 1000);
                    const limiterbacklog = new RateLimiter(5, 5000);
                    const amqp = require('amqplib/callback_api');

                    app.get('/shutdown', async (req, res) => {
                        shutdownRequested = true;
                        if (amqpConn)
                            amqpConn.close();
                        clearTimeout(startEvaluating);
                        startEvaluating = null;
                        clearTimeout(checkinTimer);
                        checkinTimer = null;
                        if (activeNode) {
                            request.get(`http://${systemglobal.Watchdog_Host}/cluster/force/search?id=${systemglobal.Cluster_ID}`, async (err, res) => {
                                if (!err && res && res.statusCode && res.statusCode < 400) {
                                    customLogger('log', "Entering Search Mode...")
                                }
                            })
                        }
                        if (!gpuLocked)
                            await processGPUWorkloads();
                        await waitForGPUUnlock();
                        shutdownComplete = true;
                        Logger.printLine("Cluter I/O", "Node has enter manual shutdown mode, Reset to rejoin cluster", "critical")
                        res.status(200).send('OK');
                    })

                    if (process.env.MQ_HOST && process.env.MQ_HOST.trim().length > 0)
                        systemglobal.mq_host = process.env.MQ_HOST.trim()
                    if (process.env.RABBITMQ_DEFAULT_USER && process.env.RABBITMQ_DEFAULT_USER.trim().length > 0)
                        systemglobal.mq_user = process.env.RABBITMQ_DEFAULT_USER.trim()
                    if (process.env.RABBITMQ_DEFAULT_PASS && process.env.RABBITMQ_DEFAULT_PASS.trim().length > 0)
                        systemglobal.mq_pass = process.env.RABBITMQ_DEFAULT_PASS.trim()

                    const mq_host = `amqp://${systemglobal.mq_user}:${systemglobal.mq_pass}@${systemglobal.mq_host}/?heartbeat=60`
                    const MQWorker1 = `${systemglobal.mq_mugino_in_bulk}`
                    const MQWorker2 = `${MQWorker1}.priority`
                    const MQWorker3 = `${MQWorker1}.backlog`

                    if (systemglobal.rules)
                        systemglobal.rules.map(async rule => { rule.channels.map(ch => { ruleSets.set(ch, rule) }) })

                    customLogger('log', ruleSets.size + ' configured rules')

                    function startWorker() {
                        amqpConn.createChannel(function(err, ch) {
                            if (closeOnErr(err)) return;
                            ch.on("error", function(err) {
                                Logger.printLine("KanmiMQ", "Channel 1 Error (Remote)", "error", err)
                            });
                            ch.on("close", function() {
                                Logger.printLine("KanmiMQ", "Channel 1 Closed (Remote)", "critical")
                                if (!shutdownRequested)
                                    amqpConn.close();
                            });
                            ch.prefetch(10);
                            ch.assertQueue(MQWorker1, { durable: true }, function(err, _ok) {
                                if (closeOnErr(err)) return;
                                ch.consume(MQWorker1, processMsg, { noAck: false });
                                Logger.printLine("KanmiMQ", "Channel 1 Worker Ready (Remote)", "debug")
                            });
                            ch.assertExchange("kanmi.exchange", "direct", {}, function(err, _ok) {
                                if (closeOnErr(err)) return;
                                ch.bindQueue(MQWorker1, "kanmi.exchange", MQWorker1, [], function(err, _ok) {
                                    if (closeOnErr(err)) return;
                                    Logger.printLine("KanmiMQ", "Channel 1 Worker Bound to Exchange (Remote)", "debug")
                                })
                            })
                            function processMsg(msg) {
                                work(msg, 'normal', function (ok) {
                                    try {
                                        if (ok)
                                            ch.ack(msg);
                                        else
                                            ch.reject(msg, true);
                                    } catch (e) {
                                        closeOnErr(e);
                                    }
                                });
                            }
                        });
                    }
                    // Priority Requests
                    function startWorker2() {
                        amqpConn.createChannel(function(err, ch) {
                            if (closeOnErr(err)) return;
                            ch.on("error", function(err) {
                                Logger.printLine("KanmiMQ", "Channel 2 Error (Local)", "error", err)
                            });
                            ch.on("close", function() {
                                Logger.printLine("KanmiMQ", "Channel 2 Closed (Local)", "critical")
                                if (!shutdownRequested)
                                    amqpConn.close();
                            });
                            ch.prefetch(1);
                            ch.assertQueue(MQWorker2, { durable: true }, function(err, _ok) {
                                if (closeOnErr(err)) return;
                                ch.consume(MQWorker2, processMsg, { noAck: false });
                                Logger.printLine("KanmiMQ", "Channel 2 Worker Ready (Local)", "debug")
                            });
                            ch.assertExchange("kanmi.exchange", "direct", {}, function(err, _ok) {
                                if (closeOnErr(err)) return;
                                ch.bindQueue(MQWorker2, "kanmi.exchange", MQWorker2, [], function(err, _ok) {
                                    if (closeOnErr(err)) return;
                                    Logger.printLine("KanmiMQ", "Channel 2 Worker Bound to Exchange (Local)", "debug")
                                })
                            })
                            function processMsg(msg) {
                                work(msg, 'priority', function (ok) {
                                    try {
                                        if (ok)
                                            ch.ack(msg);
                                        else
                                            ch.reject(msg, true);
                                    } catch (e) {
                                        closeOnErr(e);
                                    }
                                });
                            }
                        });
                    }
                    // Backlog Requests
                    function startWorker3() {
                        amqpConn.createChannel(function(err, ch) {
                            if (closeOnErr(err)) return;
                            ch.on("error", function(err) {
                                Logger.printLine("KanmiMQ", "Channel 3 Error (Backlog)", "error", err)
                            });
                            ch.on("close", function() {
                                Logger.printLine("KanmiMQ", "Channel 3 Closed (Backlog)", "critical")
                                if (!shutdownRequested)
                                    amqpConn.close();
                            });
                            ch.prefetch(5);
                            ch.assertQueue(MQWorker3, { durable: true, queueMode: 'lazy'  }, function(err, _ok) {
                                if (closeOnErr(err)) return;
                                ch.consume(MQWorker3, processMsg, { noAck: false });
                                Logger.printLine("KanmiMQ", "Channel 3 Worker Ready (Backlog)", "debug")
                            });
                            ch.assertExchange("kanmi.exchange", "direct", {}, function(err, _ok) {
                                if (closeOnErr(err)) return;
                                ch.bindQueue(MQWorker3, "kanmi.exchange", MQWorker3, [], function(err, _ok) {
                                    if (closeOnErr(err)) return;
                                    Logger.printLine("KanmiMQ", "Channel 3 Worker Bound to Exchange (Backlog)", "debug")
                                })
                            })
                            function processMsg(msg) {
                                work(msg, 'backlog', function (ok) {
                                    try {
                                        if (ok)
                                            ch.ack(msg);
                                        else
                                            ch.reject(msg, true);
                                    } catch (e) {
                                        closeOnErr(e);
                                    }
                                });
                            }
                        });
                    }
                    async function work(raw, queue, cb) {
                        try {
                            const msg = JSON.parse(Buffer.from(raw.content).toString('utf-8'));
                            const fileId = globalRunKey + '-' + DiscordSnowflake.generate();
                            /*customLogger('log', {
                                ...msg,
                                itemFileData: (msg.itemFileData) ? 'true' : 'false'
                            })*/

                            if (msg.messageType === 'command' && msg.messageEID) {
                                Logger.printLine(`MessageProcessor`, `Command Message: (${queue}) Action: ${msg.messageAction}, From: ${msg.fromClient}, To Channel: ${msg.messageChannelID}`, "info");
                                switch (msg.messageAction) {
                                    case 'Upscale':
                                        db.safe(`SELECT x.*, y.data FROM (SELECT r.*, m.url, m.valid FROM (SELECT kanmi_records.* FROM kanmi_records WHERE kanmi_records.eid = ? AND kanmi_records.source = 0) r LEFT JOIN (SELECT url, valid, fileid FROM discord_multipart_files) m ON r.fileid = m.fileid) x LEFT OUTER JOIN (SELECT * FROM kanmi_records_extended) y ON (x.eid = y.eid)`, [MessageContents.messageEID], function (err, cacheresponse) {
                                            if (err || cacheresponse.length === 0) {
                                                Logger.printLine("MPFDownload", `File not found!`, "error")
                                                cb(true)
                                            } else if (cacheresponse[0].fileid && cacheresponse.filter(e => e.valid === 0 && !(!e.url)).length !== 0) {
                                                Logger.printLine("MPFDownload", `Failed to proccess the MultiPart File ${cacheresponse.real_filename} (${MessageContents.fileUUID})\nSome files are not valid and will need to be revalidated or repaired!`, "error")
                                                cb(true)
                                            } else if (cacheresponse[0].fileid && cacheresponse.filter(e => e.valid === 1 && !(!e.url)).length !== cacheresponse[0].paritycount) {
                                                Logger.printLine("MPFDownload", `Failed to proccess the MultiPart File ${cacheresponse.real_filename} (${MessageContents.fileUUID})\nThe expected number of parity files were not available. \nTry to repair the parity cache \`juzo jfs repair parts\``, "error")
                                                cb(true)
                                            } else if (cacheresponse[0].fileid && ['jpg', 'jpeg', 'jfif', 'png'].indexOf(cacheresponse[0].real_filename.split('.').pop().toLowerCase()) !== -1) {
                                                let itemsCompleted = [];
                                                const fileName = `upscale-${fileId}.${cacheresponse[0].real_filename.split('.').pop().toLowerCase()}`
                                                const CompleteFilename = path.join(systemglobal.waifu2x_input_path, fileName);
                                                const PartsFilePath = path.join(systemglobal.mpf_temp, `PARITY-${cacheresponse[0].fileid}`);
                                                fs.mkdirSync(PartsFilePath, {recursive: true})
                                                let requests = cacheresponse.filter(e => e.valid === 1 && !(!e.url)).map(e => e.url).sort((x, y) => (x.split('.').pop() < y.split('.').pop()) ? -1 : (y.split('.').pop() > x.split('.').pop()) ? 1 : 0).reduce((promiseChain, URLtoGet, URLIndex) => {
                                                    return promiseChain.then(() => new Promise((resolve) => {
                                                        const DestFilename = path.join(PartsFilePath, `${URLIndex}.par`)
                                                        const stream = request.get({
                                                            url: URLtoGet,
                                                            headers: {
                                                                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                                                'accept-language': 'en-US,en;q=0.9',
                                                                'cache-control': 'max-age=0',
                                                                'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
                                                                'sec-ch-ua-mobile': '?0',
                                                                'sec-fetch-dest': 'document',
                                                                'sec-fetch-mode': 'navigate',
                                                                'sec-fetch-site': 'none',
                                                                'sec-fetch-user': '?1',
                                                                'upgrade-insecure-requests': '1',
                                                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.73'
                                                            },
                                                        }).pipe(fs.createWriteStream(DestFilename))
                                                        // Write File to Temp Filesystem
                                                        stream.on('finish', function () {
                                                            Logger.printLine("MPFDownload", `Downloaded Part #${URLIndex} : ${DestFilename}`, "debug", {
                                                                URL: URLtoGet,
                                                                DestFilename: DestFilename,
                                                                CompleteFilename: fileName
                                                            })
                                                            itemsCompleted.push(DestFilename);
                                                            resolve()
                                                        });
                                                        stream.on("error", function (err) {
                                                            Logger.printLine("MPFDownload", `Part of the multipart file failed to download! ${URLtoGet}`, "err", "MPFDownload", "error", err)
                                                            resolve()
                                                        })
                                                    }))
                                                }, Promise.resolve());
                                                requests.then(async () => {
                                                    if (itemsCompleted.length === cacheresponse[0].paritycount) {
                                                        await new Promise((deleted) => {
                                                            rimraf(CompleteFilename, function (err) { deleted(!err) });
                                                        })
                                                        try {
                                                            await splitFile.mergeFiles(itemsCompleted.sort(function (a, b) {
                                                                return a - b
                                                            }), CompleteFilename)
                                                            Logger.printLine("MPFDownload", `File "${fileName.replace(/[/\\?%*:|"<> ]/g, '_')}" was build successfully!`, "info")
                                                            await new Promise((deleted) => {
                                                                rimraf(PartsFilePath, function (err) { deleted(!err) });
                                                            })

                                                            cb(true)
                                                        } catch (err) {
                                                            Logger.printLine("MPFDownload", `File ${cacheresponse[0].real_filename} failed to rebuild!`, "err", err)
                                                            customLogger('error', err)
                                                            for (let part of itemsCompleted) {
                                                                fs.unlink(part, function (err) {
                                                                    if (err && (err.code === 'EBUSY' || err.code === 'ENOENT')) {
                                                                        //mqClient.sendMessage(`Error removing file part from temporary folder! - ${err.message}`, "err", "MPFDownload", err)
                                                                    }
                                                                })
                                                            }
                                                            cb(true)
                                                        }
                                                    } else {
                                                        Logger.printLine("MPFDownload", `Failed to proccess the MultiPart File ${fileId} (${MessageContents.fileUUID})\nThe expected number of parity files did not all download or save.`, "error")
                                                        cb(true)
                                                    }
                                                })
                                            } else if (!cacheresponse[0].fileid && ['jpg', 'jpeg', 'jfif', 'png'].indexOf(cacheresponse[0].real_filename.split('.').pop().toLowerCase()) !== -1) {
                                                UpscaleQueue.setItem(fileId, { id: fileId, queue, message: msg })
                                                    .then(async function () {
                                                        const URLtoGet = (( cacheresponse[0].cache_proxy) ? cacheresponse[0].cache_proxy.startsWith('http') ? cacheresponse[0].cache_proxy : `https://cdn.discordapp.com/attachments${cacheresponse[0].cache_proxy}` : (cacheresponse[0].attachment_hash && cacheresponse[0].attachment_name) ? `https://cdn.discordapp.com/attachments/` + ((cacheresponse[0].attachment_hash.includes('/')) ? cacheresponse[0].attachment_hash : `${cacheresponse[0].channel}/${cacheresponse[0].attachment_hash}/${cacheresponse[0].attachment_name}`) : undefined) + ''
                                                        const filePath = path.join(systemglobal.waifu2x_input_path, `upscale-${fileId}.${cacheresponse[0].real_filename.split('.').pop().toLowerCase()}`)
                                                        const stream = request.get({
                                                            url: URLtoGet,
                                                            headers: {
                                                                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                                                'accept-language': 'en-US,en;q=0.9',
                                                                'cache-control': 'max-age=0',
                                                                'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
                                                                'sec-ch-ua-mobile': '?0',
                                                                'sec-fetch-dest': 'document',
                                                                'sec-fetch-mode': 'navigate',
                                                                'sec-fetch-site': 'none',
                                                                'sec-fetch-user': '?1',
                                                                'upgrade-insecure-requests': '1',
                                                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.73'
                                                            },
                                                        }).pipe(fs.createWriteStream(filePath))
                                                        // Write File to Temp Filesystem
                                                        stream.on('finish', async function () {
                                                            cb(true);
                                                            if (!gpuLocked && startEvaluating === null) {
                                                                clearTimeout(startEvaluating);
                                                                startEvaluating = null;
                                                                startEvaluating = setTimeout(processGPUWorkloads, 60000)
                                                            }
                                                        });
                                                        stream.on("error", function (err) {
                                                            Logger.printLine("MPFDownload", `File failed to download! ${URLtoGet}`, "error", err)
                                                            cb(true)
                                                        })
                                                    })
                                                    .catch(function (err) {
                                                        Logger.printLine(`MessageProcessor`, `Failed to set save message`, `error`, err)
                                                        cb(false);
                                                    })
                                            } else {
                                                Logger.printLine("MPFDownload", `File format is not supported!`, "error")
                                                cb(true)
                                            }
                                        })
                                        break;
                                    default:
                                        cb(true);
                                        break;
                                }
                            } else if (msg.messageType === 'sfile' && msg.itemFileData && msg.itemFileName && ['jpg', 'jpeg', 'jfif', 'png'].indexOf(msg.itemFileName.split('.').pop().toLowerCase()) !== -1) {
                                Logger.printLine(`MessageProcessor`, `Process Message: (${queue}) From: ${msg.fromClient}, To Channel: ${msg.messageChannelID}`, "info");
                                LocalQueue.setItem(fileId, { id: fileId, queue, message: msg })
                                    .then(async function () {
                                        let image = sharp(new Buffer.from(msg.itemFileData, 'base64'));
                                        const metadata = await image.metadata();
                                        const rules = ruleSets.get(msg.messageChannelID);
                                        const valid = (() => {
                                            let smallest = null;
                                            let largest = null;
                                            if (metadata.width > metadata.height) { // Landscape Resize
                                                largest = metadata.width;
                                                smallest = metadata.height;
                                            } else { // Portrait or Square Image
                                                largest = metadata.height;
                                                smallest = metadata.width;
                                            }
                                            if (rules && metadata && rules.require && rules.require.max_res && rules.require.max_res <= largest) {
                                                customLogger('error', `Blocked because image to large: ${largest} > ${rules.require.max_res} `);
                                                return false;
                                            }
                                            if (rules && metadata && rules.require && rules.require.min_res && rules.require.min_res > smallest) {
                                                customLogger('error', `Blocked because image to small: ${smallest} < ${rules.require.min_res}"`);
                                                return false;
                                            }
                                            if (rules && metadata && rules.require && rules.require.not_aspect_ratio && rules.require.not_aspect_ratio.indexOf(toFixed((metadata.height / metadata.width), 5).toString()) !== -1) {
                                                customLogger('error', `Blocked because aspect ratio: ${toFixed((metadata.height / metadata.width), 5).toString()}R"`);
                                                return false;
                                            }
                                            return true;
                                        })()
                                        if (valid) {
                                            await image
                                                .toFormat('png')
                                                .withMetadata()
                                                .toFile(path.join(systemglobal.holding_path || systemglobal.deepbooru_input_path, `message-${fileId}.png`), (err, info) => {
                                                    if (err) {
                                                        Logger.printLine("SaveFile", `Error when saving the file ${fileId}`, "error")
                                                        customLogger('error', err);
                                                        mqClient.sendData(`${systemglobal.mq_discord_out}${(queue !== 'normal') ? '.' + queue : ''}`, msg, function (ok) {
                                                            cb(ok);
                                                        });
                                                    } else {
                                                        if (!gpuLocked && startEvaluating === null) {
                                                            clearTimeout(startEvaluating);
                                                            startEvaluating = null;
                                                            startEvaluating = setTimeout(processGPUWorkloads, 60000)
                                                        }
                                                        cb(true);
                                                    }
                                                })
                                        } else {
                                            Logger.printLine(`MessageProcessor`, `Image was rejected by pre-parser`, `warn`)
                                            cb(true);
                                        }
                                        image = null;
                                    })
                                    .catch(function (err) {
                                        customLogger('log', err);
                                        Logger.printLine(`MessageProcessor`, `Failed to save message`, `error`, err)
                                        mqClient.sendData( `${systemglobal.mq_discord_out}${(queue !== 'normal') ? '.' + queue : ''}`, msg, function (ok) {
                                            cb(ok);
                                        });
                                    })
                            } else {
                                Logger.printLine(`MessageProcessor`, `Bypass Message: (${queue}) From: ${msg.fromClient}, To Channel: ${msg.messageChannelID}`, "debug");
                                mqClient.sendData( `${systemglobal.mq_discord_out}${(queue !== 'normal') ? '.' + queue : ''}`, msg, function (ok) {
                                    cb(ok);
                                });
                            }
                        } catch (err) {
                            Logger.printLine("JobParser", "Error Parsing Job - " + err.message, "critical")
                            cb(false);
                        }
                    }
                    function start() {
                        amqp.connect(mq_host, function(err, conn) {
                            if (err) {
                                Logger.printLine("KanmiMQ", "Initialization Error", "critical", err)
                                return setTimeout(start, 1000);
                            }
                            conn.on("error", function(err) {
                                if (err.message !== "Connection closing") {
                                    Logger.printLine("KanmiMQ", "Initialization Connection Error", "emergency", err)
                                }
                            });
                            conn.on("close", function() {
                                if ((active || systemglobal.mq_mugino_in_bulk) && !shutdownRequested) {
                                    Logger.printLine("KanmiMQ", "Attempting to Reconnect...", "debug")
                                    return setTimeout(start, 1000);
                                }
                            });
                            Logger.printLine("KanmiMQ", `Connected to Kanmi Exchange as ${systemglobal.system_name}!`, "info")
                            amqpConn = conn;
                            whenConnected();
                        });
                    }
                    function closeOnErr(err) {
                        if (!err) return false;
                        customLogger('error', err)
                        Logger.printLine("KanmiMQ", "Connection Closed due to error", "error", err)
                        amqpConn.close();
                        return true;
                    }
                    async function whenConnected() {
                        startWorker();
                        startWorker2();
                        startWorker3();
                        if (process.send && typeof process.send === 'function')
                            process.send('ready');
                        init = true
                    }

                    watchResults();
                    startServer();
                    await processGPUWorkloads();
                    start();
                    if (systemglobal.search)
                        await parseUntilDone(systemglobal.search);
                    customLogger('log', "First pass completed!")
                } else {
                    shutdownComplete = true;
                    watchResults();
                    await parseUntilDone(undefined, true);
                    app.get('/shutdown', async (req, res) => {
                        clearTimeout(checkinTimer);
                        checkinTimer = null;
                        shutdownComplete = true;
                        res.status(200).send('OK');
                        Logger.printLine("Cluter I/O", "Node has enter manual shutdown mode, Reset to rejoin cluster", "critical")
                    })
                    startServer();
                    if (systemglobal.fan_reset_url) {
                        setInterval(() => {
                            request.get(`http://${systemglobal.fan_reset_url}`, async (err, res, body) => {
                                if (err || res && res.statusCode !== undefined && res.statusCode !== 200) {
                                    customLogger('error', err || res.body);
                                }
                            })
                        }, 60000)
                    }
                }
            } else {
                Logger.printLine("ClusterIO", "System active master", "info");
                activeNode = true;
                cont(true)
            }
        })
    }

    function startServer() {
        app.get('/stats', async (req, res) => {
            res.status(200).json({
                uptime: ((Date.now() - bootTime) / 60000).toFixed(2),
                total: totalItems,
                past_jobs: pastJobs,
            });
        })
        app.get('/reset', async (req, res) => {
            shutdownRequested = true;
            if (amqpConn)
                amqpConn.close();
            clearTimeout(startEvaluating);
            startEvaluating = null;
            if (!gpuLocked)
                await processGPUWorkloads();
            await waitForGPUUnlock();
            shutdownComplete = true;
            clearTimeout(checkinTimer);
            checkinTimer = null;
            res.status(200).send('Bye');
            process.exit(1);
        })
        app.get('/', async (req, res) => {
            res.status(200).send("Mugino MIITS!");
        })
        const server = app.listen(9052, '0.0.0.0',async function (err) {
            if (err) {
                customLogger('log', 'App listening error ', err);
            } else {
                customLogger('log', 'App running at 9052')
            }
        });
    }
    async function waitForGPUUnlock() {
        while(gpuLocked) {
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
        await new Promise(resolve => setTimeout(resolve, 15000));
    }
    function toFixed( v, d ) {
        //return (+(Math.round(+(v + 'e' + d)) + 'e' + -d)).toFixed(d);
        return parseFloat(Math.round(v.toFixed(d+1)+'e'+d)+'e-'+d)
    }

    watchResults();
    if (systemglobal.mq_mugino_in) {
        const RateLimiter = require('limiter').RateLimiter;
        const limiter = new RateLimiter(5, 5000);
        const limiterlocal = new RateLimiter(1, 1000);
        const limiterbacklog = new RateLimiter(5, 5000);
        const amqp = require('amqplib/callback_api');

        app.get('/shutdown', async (req, res) => {
            shutdownRequested = true;
            if (amqpConn)
                amqpConn.close();
            clearTimeout(startEvaluating);
            startEvaluating = null;
            clearTimeout(checkinTimer);
            checkinTimer = null;
            if (activeNode) {
                request.get(`http://${systemglobal.Watchdog_Host}/cluster/force/search?id=${systemglobal.Cluster_ID}`, async (err, res) => {
                    if (!err && res && res.statusCode && res.statusCode < 400) {
                        customLogger('log', "Entering Search Mode...")
                    }
                })
            }
            if (!gpuLocked)
                await processGPUWorkloads();
            await waitForGPUUnlock();
            shutdownComplete = true;
            Logger.printLine("Cluter I/O", "Node has enter manual shutdown mode, Reset to rejoin cluster", "critical")
            res.status(200).send('OK');
        })

        if (process.env.MQ_HOST && process.env.MQ_HOST.trim().length > 0)
            systemglobal.mq_host = process.env.MQ_HOST.trim()
        if (process.env.RABBITMQ_DEFAULT_USER && process.env.RABBITMQ_DEFAULT_USER.trim().length > 0)
            systemglobal.mq_user = process.env.RABBITMQ_DEFAULT_USER.trim()
        if (process.env.RABBITMQ_DEFAULT_PASS && process.env.RABBITMQ_DEFAULT_PASS.trim().length > 0)
            systemglobal.mq_pass = process.env.RABBITMQ_DEFAULT_PASS.trim()

        const mq_host = `amqp://${systemglobal.mq_user}:${systemglobal.mq_pass}@${systemglobal.mq_host}/?heartbeat=60`
        const MQWorker1 = `${systemglobal.mq_mugino_in}`
        const MQWorker2 = `${MQWorker1}.priority`
        const MQWorker3 = `${MQWorker1}.backlog`

        if (systemglobal.rules)
            systemglobal.rules.map(async rule => { rule.channels.map(ch => { ruleSets.set(ch, rule) }) })

        customLogger('log', ruleSets.size + ' configured rules')

        function startWorker() {
            amqpConn.createChannel(function(err, ch) {
                if (closeOnErr(err)) return;
                ch.on("error", function(err) {
                    Logger.printLine("KanmiMQ", "Channel 1 Error (Remote)", "error", err)
                });
                ch.on("close", function() {
                    Logger.printLine("KanmiMQ", "Channel 1 Closed (Remote)", "critical")
                    if (!shutdownRequested)
                        amqpConn.close();
                });
                ch.prefetch(10);
                ch.assertQueue(MQWorker1, { durable: true }, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.consume(MQWorker1, processMsg, { noAck: false });
                    Logger.printLine("KanmiMQ", "Channel 1 Worker Ready (Remote)", "debug")
                });
                ch.assertExchange("kanmi.exchange", "direct", {}, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.bindQueue(MQWorker1, "kanmi.exchange", MQWorker1, [], function(err, _ok) {
                        if (closeOnErr(err)) return;
                        Logger.printLine("KanmiMQ", "Channel 1 Worker Bound to Exchange (Remote)", "debug")
                    })
                })
                function processMsg(msg) {
                    work(msg, 'normal', function (ok) {
                        try {
                            if (ok)
                                ch.ack(msg);
                            else
                                ch.reject(msg, true);
                        } catch (e) {
                            closeOnErr(e);
                        }
                    });
                }
            });
        }
        // Priority Requests
        function startWorker2() {
            amqpConn.createChannel(function(err, ch) {
                if (closeOnErr(err)) return;
                ch.on("error", function(err) {
                    Logger.printLine("KanmiMQ", "Channel 2 Error (Local)", "error", err)
                });
                ch.on("close", function() {
                    Logger.printLine("KanmiMQ", "Channel 2 Closed (Local)", "critical")
                    if (!shutdownRequested)
                        amqpConn.close();
                });
                ch.prefetch(1);
                ch.assertQueue(MQWorker2, { durable: true }, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.consume(MQWorker2, processMsg, { noAck: false });
                    Logger.printLine("KanmiMQ", "Channel 2 Worker Ready (Local)", "debug")
                });
                ch.assertExchange("kanmi.exchange", "direct", {}, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.bindQueue(MQWorker2, "kanmi.exchange", MQWorker2, [], function(err, _ok) {
                        if (closeOnErr(err)) return;
                        Logger.printLine("KanmiMQ", "Channel 2 Worker Bound to Exchange (Local)", "debug")
                    })
                })
                function processMsg(msg) {
                    work(msg, 'priority', function (ok) {
                        try {
                            if (ok)
                                ch.ack(msg);
                            else
                                ch.reject(msg, true);
                        } catch (e) {
                            closeOnErr(e);
                        }
                    });
                }
            });
        }
        // Backlog Requests
        function startWorker3() {
            amqpConn.createChannel(function(err, ch) {
                if (closeOnErr(err)) return;
                ch.on("error", function(err) {
                    Logger.printLine("KanmiMQ", "Channel 3 Error (Backlog)", "error", err)
                });
                ch.on("close", function() {
                    Logger.printLine("KanmiMQ", "Channel 3 Closed (Backlog)", "critical")
                    if (!shutdownRequested)
                        amqpConn.close();
                });
                ch.prefetch(5);
                ch.assertQueue(MQWorker3, { durable: true, queueMode: 'lazy'  }, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.consume(MQWorker3, processMsg, { noAck: false });
                    Logger.printLine("KanmiMQ", "Channel 3 Worker Ready (Backlog)", "debug")
                });
                ch.assertExchange("kanmi.exchange", "direct", {}, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.bindQueue(MQWorker3, "kanmi.exchange", MQWorker3, [], function(err, _ok) {
                        if (closeOnErr(err)) return;
                        Logger.printLine("KanmiMQ", "Channel 3 Worker Bound to Exchange (Backlog)", "debug")
                    })
                })
                function processMsg(msg) {
                    work(msg, 'backlog', function (ok) {
                        try {
                            if (ok)
                                ch.ack(msg);
                            else
                                ch.reject(msg, true);
                        } catch (e) {
                            closeOnErr(e);
                        }
                    });
                }
            });
        }
        async function work(raw, queue, cb) {
            try {
                const msg = JSON.parse(Buffer.from(raw.content).toString('utf-8'));
                const fileId = globalRunKey + '-' + DiscordSnowflake.generate();
                /*customLogger('log', {
                    ...msg,
                    itemFileData: (msg.itemFileData) ? 'true' : 'false'
                })*/

                if (msg.messageType === 'command' && msg.messageEID) {
                    Logger.printLine(`MessageProcessor`, `Command Message: (${queue}) Action: ${msg.messageAction}, From: ${msg.fromClient}, To Channel: ${msg.messageChannelID}`, "info");
                    switch (msg.messageAction) {
                        case 'Upscale':
                            db.safe(`SELECT x.*, y.data FROM (SELECT r.*, m.url, m.valid FROM (SELECT kanmi_records.* FROM kanmi_records WHERE kanmi_records.eid = ? AND kanmi_records.source = 0) r LEFT JOIN (SELECT url, valid, fileid FROM discord_multipart_files) m ON r.fileid = m.fileid) x LEFT OUTER JOIN (SELECT * FROM kanmi_records_extended) y ON (x.eid = y.eid)`, [MessageContents.messageEID], function (err, cacheresponse) {
                                if (err || cacheresponse.length === 0) {
                                    Logger.printLine("MPFDownload", `File not found!`, "error")
                                    cb(true)
                                } else if (cacheresponse[0].fileid && cacheresponse.filter(e => e.valid === 0 && !(!e.url)).length !== 0) {
                                    Logger.printLine("MPFDownload", `Failed to proccess the MultiPart File ${cacheresponse.real_filename} (${MessageContents.fileUUID})\nSome files are not valid and will need to be revalidated or repaired!`, "error")
                                    cb(true)
                                } else if (cacheresponse[0].fileid && cacheresponse.filter(e => e.valid === 1 && !(!e.url)).length !== cacheresponse[0].paritycount) {
                                    Logger.printLine("MPFDownload", `Failed to proccess the MultiPart File ${cacheresponse.real_filename} (${MessageContents.fileUUID})\nThe expected number of parity files were not available. \nTry to repair the parity cache \`juzo jfs repair parts\``, "error")
                                    cb(true)
                                } else if (cacheresponse[0].fileid && ['jpg', 'jpeg', 'jfif', 'png'].indexOf(cacheresponse[0].real_filename.split('.').pop().toLowerCase()) !== -1) {
                                    let itemsCompleted = [];
                                    const fileName = `upscale-${fileId}.${cacheresponse[0].real_filename.split('.').pop().toLowerCase()}`
                                    const CompleteFilename = path.join(systemglobal.waifu2x_input_path, fileName);
                                    const PartsFilePath = path.join(systemglobal.mpf_temp, `PARITY-${cacheresponse[0].fileid}`);
                                    fs.mkdirSync(PartsFilePath, {recursive: true})
                                    let requests = cacheresponse.filter(e => e.valid === 1 && !(!e.url)).map(e => e.url).sort((x, y) => (x.split('.').pop() < y.split('.').pop()) ? -1 : (y.split('.').pop() > x.split('.').pop()) ? 1 : 0).reduce((promiseChain, URLtoGet, URLIndex) => {
                                        return promiseChain.then(() => new Promise((resolve) => {
                                            const DestFilename = path.join(PartsFilePath, `${URLIndex}.par`)
                                            const stream = request.get({
                                                url: URLtoGet,
                                                headers: {
                                                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                                    'accept-language': 'en-US,en;q=0.9',
                                                    'cache-control': 'max-age=0',
                                                    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
                                                    'sec-ch-ua-mobile': '?0',
                                                    'sec-fetch-dest': 'document',
                                                    'sec-fetch-mode': 'navigate',
                                                    'sec-fetch-site': 'none',
                                                    'sec-fetch-user': '?1',
                                                    'upgrade-insecure-requests': '1',
                                                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.73'
                                                },
                                            }).pipe(fs.createWriteStream(DestFilename))
                                            // Write File to Temp Filesystem
                                            stream.on('finish', function () {
                                                Logger.printLine("MPFDownload", `Downloaded Part #${URLIndex} : ${DestFilename}`, "debug", {
                                                    URL: URLtoGet,
                                                    DestFilename: DestFilename,
                                                    CompleteFilename: fileName
                                                })
                                                itemsCompleted.push(DestFilename);
                                                resolve()
                                            });
                                            stream.on("error", function (err) {
                                                Logger.printLine("MPFDownload", `Part of the multipart file failed to download! ${URLtoGet}`, "err", "MPFDownload", "error", err)
                                                resolve()
                                            })
                                        }))
                                    }, Promise.resolve());
                                    requests.then(async () => {
                                        if (itemsCompleted.length === cacheresponse[0].paritycount) {
                                            await new Promise((deleted) => {
                                                rimraf(CompleteFilename, function (err) { deleted(!err) });
                                            })
                                            try {
                                                await splitFile.mergeFiles(itemsCompleted.sort(function (a, b) {
                                                    return a - b
                                                }), CompleteFilename)
                                                Logger.printLine("MPFDownload", `File "${fileName.replace(/[/\\?%*:|"<> ]/g, '_')}" was build successfully!`, "info")
                                                await new Promise((deleted) => {
                                                    rimraf(PartsFilePath, function (err) { deleted(!err) });
                                                })

                                                cb(true)
                                            } catch (err) {
                                                Logger.printLine("MPFDownload", `File ${cacheresponse[0].real_filename} failed to rebuild!`, "err", err)
                                                customLogger('error', err)
                                                for (let part of itemsCompleted) {
                                                    fs.unlink(part, function (err) {
                                                        if (err && (err.code === 'EBUSY' || err.code === 'ENOENT')) {
                                                            //mqClient.sendMessage(`Error removing file part from temporary folder! - ${err.message}`, "err", "MPFDownload", err)
                                                        }
                                                    })
                                                }
                                                cb(true)
                                            }
                                        } else {
                                            Logger.printLine("MPFDownload", `Failed to proccess the MultiPart File ${fileId} (${MessageContents.fileUUID})\nThe expected number of parity files did not all download or save.`, "error")
                                            cb(true)
                                        }
                                    })
                                } else if (!cacheresponse[0].fileid && ['jpg', 'jpeg', 'jfif', 'png'].indexOf(cacheresponse[0].real_filename.split('.').pop().toLowerCase()) !== -1) {
                                    UpscaleQueue.setItem(fileId, { id: fileId, queue, message: msg })
                                        .then(async function () {
                                            const URLtoGet = (( cacheresponse[0].cache_proxy) ? cacheresponse[0].cache_proxy.startsWith('http') ? cacheresponse[0].cache_proxy : `https://cdn.discordapp.com/attachments${cacheresponse[0].cache_proxy}` : (cacheresponse[0].attachment_hash && cacheresponse[0].attachment_name) ? `https://cdn.discordapp.com/attachments/` + ((cacheresponse[0].attachment_hash.includes('/')) ? cacheresponse[0].attachment_hash : `${cacheresponse[0].channel}/${cacheresponse[0].attachment_hash}/${cacheresponse[0].attachment_name}`) : undefined) + ''
                                            const filePath = path.join(systemglobal.waifu2x_input_path, `upscale-${fileId}.${cacheresponse[0].real_filename.split('.').pop().toLowerCase()}`)
                                            const stream = request.get({
                                                url: URLtoGet,
                                                headers: {
                                                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                                    'accept-language': 'en-US,en;q=0.9',
                                                    'cache-control': 'max-age=0',
                                                    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
                                                    'sec-ch-ua-mobile': '?0',
                                                    'sec-fetch-dest': 'document',
                                                    'sec-fetch-mode': 'navigate',
                                                    'sec-fetch-site': 'none',
                                                    'sec-fetch-user': '?1',
                                                    'upgrade-insecure-requests': '1',
                                                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.73'
                                                },
                                            }).pipe(fs.createWriteStream(filePath))
                                            // Write File to Temp Filesystem
                                            stream.on('finish', async function () {
                                                cb(true);
                                                if (!gpuLocked && startEvaluating === null) {
                                                    clearTimeout(startEvaluating);
                                                    startEvaluating = null;
                                                    startEvaluating = setTimeout(processGPUWorkloads, 60000)
                                                }
                                            });
                                            stream.on("error", function (err) {
                                                Logger.printLine("MPFDownload", `File failed to download! ${URLtoGet}`, "error", err)
                                                cb(true)
                                            })
                                        })
                                        .catch(function (err) {
                                            Logger.printLine(`MessageProcessor`, `Failed to set save message`, `error`, err)
                                            cb(false);
                                        })
                                } else {
                                    Logger.printLine("MPFDownload", `File format is not supported!`, "error")
                                    cb(true)
                                }
                            })
                            break;
                        default:
                            cb(true);
                            break;
                    }
                } else if (msg.messageType === 'sfile' && msg.itemFileData && msg.itemFileName && ['jpg', 'jpeg', 'jfif', 'png'].indexOf(msg.itemFileName.split('.').pop().toLowerCase()) !== -1) {
                    Logger.printLine(`MessageProcessor`, `Process Message: (${queue}) From: ${msg.fromClient}, To Channel: ${msg.messageChannelID}`, "info");
                    LocalQueue.setItem(fileId, { id: fileId, queue, message: msg })
                        .then(async function () {
                            let image = sharp(new Buffer.from(msg.itemFileData, 'base64'));
                            const metadata = await image.metadata();
                            const rules = ruleSets.get(msg.messageChannelID);
                            const valid = (() => {
                                let smallest = null;
                                let largest = null;
                                if (metadata.width > metadata.height) { // Landscape Resize
                                    largest = metadata.width;
                                    smallest = metadata.height;
                                } else { // Portrait or Square Image
                                    largest = metadata.height;
                                    smallest = metadata.width;
                                }
                                if (rules && metadata && rules.require && rules.require.max_res && rules.require.max_res <= largest) {
                                    customLogger('error', `Blocked because image to large: ${largest} > ${rules.require.max_res} `);
                                    return false;
                                }
                                if (rules && metadata && rules.require && rules.require.min_res && rules.require.min_res > smallest) {
                                    customLogger('error', `Blocked because image to small: ${smallest} < ${rules.require.min_res}"`);
                                    return false;
                                }
                                if (rules && metadata && rules.require && rules.require.not_aspect_ratio && rules.require.not_aspect_ratio.indexOf(toFixed((metadata.height / metadata.width), 5).toString()) !== -1) {
                                    customLogger('error', `Blocked because aspect ratio: ${toFixed((metadata.height / metadata.width), 5).toString()}R"`);
                                    return false;
                                }
                                return true;
                            })()
                            if (valid) {
                                await image
                                    .toFormat('png')
                                    .withMetadata()
                                    .toFile(path.join(systemglobal.holding_path || systemglobal.deepbooru_input_path, `message-${fileId}.png`), (err, info) => {
                                        if (err) {
                                            Logger.printLine("SaveFile", `Error when saving the file ${fileId}`, "error")
                                            customLogger('error', err);
                                            mqClient.sendData(`${systemglobal.mq_discord_out}${(queue !== 'normal') ? '.' + queue : ''}`, msg, function (ok) {
                                                cb(ok);
                                            });
                                        } else {
                                            if (!gpuLocked && startEvaluating === null) {
                                                clearTimeout(startEvaluating);
                                                startEvaluating = null;
                                                startEvaluating = setTimeout(processGPUWorkloads, 60000)
                                            }
                                            cb(true);
                                        }
                                    })
                            } else {
                                Logger.printLine(`MessageProcessor`, `Image was rejected by pre-parser`, `warn`)
                                cb(true);
                            }
                            image = null;
                        })
                        .catch(function (err) {
                            customLogger('log', err);
                            Logger.printLine(`MessageProcessor`, `Failed to save message`, `error`, err)
                            mqClient.sendData( `${systemglobal.mq_discord_out}${(queue !== 'normal') ? '.' + queue : ''}`, msg, function (ok) {
                                cb(ok);
                            });
                        })
                } else {
                    Logger.printLine(`MessageProcessor`, `Bypass Message: (${queue}) From: ${msg.fromClient}, To Channel: ${msg.messageChannelID}`, "debug");
                    mqClient.sendData( `${systemglobal.mq_discord_out}${(queue !== 'normal') ? '.' + queue : ''}`, msg, function (ok) {
                        cb(ok);
                    });
                }
            } catch (err) {
                Logger.printLine("JobParser", "Error Parsing Job - " + err.message, "critical")
                cb(false);
            }
        }
        function start() {
            amqp.connect(mq_host, function(err, conn) {
                if (err) {
                    Logger.printLine("KanmiMQ", "Initialization Error", "critical", err)
                    return setTimeout(start, 1000);
                }
                conn.on("error", function(err) {
                    if (err.message !== "Connection closing") {
                        Logger.printLine("KanmiMQ", "Initialization Connection Error", "emergency", err)
                    }
                });
                conn.on("close", function() {
                    if (active && !shutdownRequested) {
                        Logger.printLine("KanmiMQ", "Attempting to Reconnect...", "debug")
                        return setTimeout(start, 1000);
                    }
                });
                Logger.printLine("KanmiMQ", `Connected to Kanmi Exchange as ${systemglobal.system_name}!`, "info")
                amqpConn = conn;
                whenConnected();
            });
        }
        function closeOnErr(err) {
            if (!err) return false;
            customLogger('error', err)
            Logger.printLine("KanmiMQ", "Connection Closed due to error", "error", err)
            amqpConn.close();
            return true;
        }
        async function whenConnected() {
            startWorker();
            startWorker2();
            startWorker3();
            if (process.send && typeof process.send === 'function')
                process.send('ready');
            init = true
        }

        startServer();
        await processGPUWorkloads();
        start();
        if (systemglobal.search)
            await parseUntilDone(systemglobal.search);
        customLogger('log', "First pass completed!")
    } else {
        await processGPUWorkloads();
        if (systemglobal.search)
            await parseUntilDone(systemglobal.search);
        customLogger('log', "First pass completed!")
    }

    async function clearFolder(folderPath) {
        try {
            const files = await fs.promises.readdir(folderPath);
            for (const file of files) {
                await fs.promises.unlink(path.resolve(folderPath, file));
                customLogger('log', `${folderPath}/${file} has been removed successfully`);
            }
        } catch (err){
            customLogger('log', err);
        }
    }
    // On-The-Fly Tagging System (aka no wasted table space)
    async function addTagForEid(eid, name, rating = 0) {
        let tagId = 0;
        const type = modelTags.get(name) || 0;
        if (!exsitingTags.has(name)) {
            await sqlPromiseSafe(`INSERT INTO sequenzia_index_tags SET name = ?, type = ?`, [name, type]);
            const newId = (await sqlPromiseSafe(`SELECT id, name FROM sequenzia_index_tags WHERE name = ?`, [name])).rows[0]
            tagId = newId.id;
            exsitingTags.set(name, tagId);
        } else {
            tagId = exsitingTags.get(name);
        }
        await sqlPromiseSafe(`INSERT INTO sequenzia_index_matches SET tag_pair = ?, eid = ?, tag = ?, rating = ? ON DUPLICATE KEY UPDATE rating = ?`, [
            parseInt(eid.toString() + tagId.toString()),
            eid, tagId, rating, rating
        ])
    }
    async function processGPUWorkloads() {
        clearTimeout(startEvaluating);
        startEvaluating = null;
        if (!gpuLocked) {
            await validateImageInputs();
            if (systemglobal.deepbooru_exec)
                await queryImageTags();
            if (systemglobal.waifu2x_exec)
                await upscaleImages();
        } else {
            return false;
        }
    }
    async function upscaleImages() {
        if (!upscaleIsActive) {
            upscaleIsActive = true;
            customLogger('log', 'Processing image upscale via MIITS Client...')
            return new Promise(async (resolve) => {
                const startTime = Date.now();
                (fs.readdirSync(systemglobal.waifu2x_input_path))
                    .filter(e => fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.png`)) || fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.jpg`)) || (fs.statSync(path.resolve(systemglobal.waifu2x_input_path, e))).size <= 16)
                    .map(e => fs.unlinkSync(path.resolve(systemglobal.waifu2x_input_path, e)))
                if ((fs.readdirSync(systemglobal.waifu2x_input_path)).length > 0) {
                    await new Promise(completed => {
                        let requests = (fs.readdirSync(systemglobal.waifu2x_input_path)).reduce((promiseChain, e) => {
                            return promiseChain.then(() => new Promise(async (resolve) => {
                                const key = e.split('.')[0].split('upscale-').pop();
                                UpscaleQueue.getItem(key)
                                    .then(data => {
                                        if (data.parameters) {
                                            let w2xOptions = [
                                                (systemglobal.waifu2x_exec_input_parameter || '-i'),
                                                path.join(systemglobal.waifu2x_input_path, e),
                                                (systemglobal.waifu2x_exec_output_parameter || '-o'),
                                                path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.${systemglobal.waifu2x_exec_format_options[data.parameters.image_format || 0]}`),
                                                (systemglobal.waifu2x_exec_noise_parameter || '-n'),
                                                systemglobal.waifu2x_exec_noise_options[data.parameters.noise_level || 0],
                                                (systemglobal.waifu2x_exec_size_parameter || '-s'),
                                                systemglobal.waifu2x_exec_size_options[data.parameters.scale_level || 0],
                                                (systemglobal.waifu2x_exec_format_parameter || '-f'),
                                                systemglobal.waifu2x_exec_format_options[data.parameters.image_format || 0],
                                                ...(systemglobal.waifu2x_exec_additional_options || [])
                                            ]
                                            customLogger('log', w2xOptions.join(' '))
                                            const muginoMeltdown = spawn(((systemglobal.waifu2x_exec) ? systemglobal.waifu2x_exec : 'waifu2x'), w2xOptions, {encoding: 'utf8'})
                                            if (!systemglobal.waifu2x_no_log)
                                                muginoMeltdown.stdout.on('data', (data) => customLogger('log', data.toString().trim().split('\n').filter(e => e.trim().length > 1 && !e.trim().includes('===] ')).join('\n')))
                                            muginoMeltdown.stderr.on('data', (data) => customLogger('error', data.toString()));
                                            muginoMeltdown.on('close', (code, signal) => {
                                                (fs.readdirSync(systemglobal.waifu2x_input_path))
                                                    .filter(e => fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.jpg`)) || fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.png`)))
                                                    .map(e => fs.unlinkSync(path.resolve(systemglobal.waifu2x_input_path, e)))
                                                if (code !== 0) {
                                                    customLogger('error', `Mugino Meltdown! MIITS Upscaler reported a error during upscale operation!`);
                                                    upscaleIsActive = false;
                                                    resolve(false)
                                                } else {
                                                    customLogger('log', `Tagging Completed in ${((Date.now() - startTime) / 1000).toFixed(0)} sec!`);
                                                    upscaleIsActive = false;
                                                    resolve(true)
                                                }
                                            })
                                        } else {
                                            customLogger('error', `Unexpectedly Failed to get message data for key ${key}`)
                                            upscaleIsActive = false;
                                            resolve(true);
                                        }
                                    })
                                    .catch(err => {
                                        customLogger('error', `Unexpectedly Failed to get message for key ${key}`, err)
                                        upscaleIsActive = false;
                                        resolve(true);
                                    })
                            }))
                        }, Promise.resolve());
                        requests.then(async () => {
                            completed();
                        })
                    })
                } else {
                    console.info(`There are no file that need to be upscaled!`);
                    mittsIsActive = false;
                    resolve(true)
                }
            })
        } else {
            return true;
        }
    }
    async function queryImageTags() {
        if (!mittsIsActive) {
            mittsIsActive = true;
            customLogger('log', 'Processing image tags via MIITS Client...')
            const date = Date.now();
            return new Promise(async (resolve) => {
                const startTime = Date.now();
                (fs.readdirSync(systemglobal.deepbooru_input_path))
                    .filter(e => fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.json`)) || (fs.statSync(path.resolve(systemglobal.deepbooru_input_path, e))).size <= 16)
                    .map(e => {
                        try {
                            fs.unlinkSync(path.resolve(systemglobal.deepbooru_input_path, e))
                        } catch (e) {

                        }
                    })
                const items = (fs.readdirSync(systemglobal.deepbooru_input_path)).length;
                if (items > 0) {
                    if (systemglobal.fan_up_url) {
                        request.get(`http://${systemglobal.fan_up_url}`, async (err, res, body) => {
                            if (err || res && res.statusCode !== undefined && res.statusCode !== 200) {
                                customLogger('error', err || res.body);
                            }
                        })
                    }
                    let ddOptions = ['evaluate', systemglobal.deepbooru_input_path, '--project-path', systemglobal.deepbooru_model_path, '--allow-folder', '--save-json', '--save-path', systemglobal.deepbooru_output_path, '--no-tag-output']
                    if (systemglobal.deepbooru_gpu)
                        ddOptions.push('--allow-gpu')
                    customLogger('log', ddOptions.join(' '))
                    const muginoMeltdown = spawn(((systemglobal.deepbooru_exec) ? systemglobal.deepbooru_exec : 'deepbooru'), ddOptions, {encoding: 'utf8'})

                    if (!systemglobal.deepbooru_no_log)
                        muginoMeltdown.stdout.on('data', (data) => customLogger('log', data.toString().trim().split('\n').filter(e => e.trim().length > 1 && !e.trim().includes('===] ')).join('\n')))
                    muginoMeltdown.stderr.on('data', (data) => customLogger('error', data.toString()));
                    muginoMeltdown.on('close', (code, signal) => {
                        /*(fs.readdirSync(systemglobal.deepbooru_input_path))
                            .filter(e => fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.json`)))
                            .map(e => fs.unlinkSync(path.resolve(systemglobal.deepbooru_input_path, e)))*/
                        if (code !== 0) {
                            customLogger('error', `Mugino Meltdown! MIITS reported a error during tagging operation!`);
                            mittsIsActive = false;
                            totalItems += 1
                            resolve(false)
                        } else {
                            customLogger('log', `Tagging Completed in ${((Date.now() - startTime) / 1000).toFixed(0)} sec!`);
                            mittsIsActive = false;
                            resolve(true)
                        }
                        if (systemglobal.fan_reset_url) {
                            setInterval(() => {
                                request.get(`http://${systemglobal.fan_reset_url}`, async (err, res, body) => {
                                    if (err || res && res.statusCode !== undefined && res.statusCode !== 200) {
                                        customLogger('error', err || res.body);
                                    }
                                })
                            }, 60000)
                        }
                    })
                } else {
                    console.info(`There are no file that need to be tagged!`);
                    mittsIsActive = false;
                    resolve(true)
                }
                pastJobs.push({
                    date,
                    items
                })
                totalItems += items || 1
            })
        } else {
            return true;
        }
    }
    async function queryForTags(analyzerGroup) {
        const sqlFields = [
            'kanmi_records.id',
            'kanmi_records.eid',
            'kanmi_records.channel',
            'kanmi_records.attachment_name',
            'kanmi_records.attachment_hash',
            'kanmi_records.attachment_auth',
            'IF(kanmi_records.attachment_auth_ex > NOW() + INTERVAL 1 HOUR, 1, 0) AS attachment_auth_valid'
        ]
        const sqlTables = [
            'kanmi_records',
            'kanmi_channels',
        ]
        const sqlWhereBase = [
            'kanmi_records.channel = kanmi_channels.channelid',
            'kanmi_records.attachment_hash IS NOT NULL',
            'kanmi_records.attachment_name IS NOT NULL',
            'kanmi_records.attachment_extra IS NULL',
            'kanmi_records.flagged = 0',
            'kanmi_records.hidden = 0',
            'kanmi_records.tags IS NULL',
        ]
        const sqlWhereFiletypes = [
            "kanmi_records.attachment_name LIKE '%.jp%_'",
            "kanmi_records.attachment_name LIKE '%.jfif'",
            "kanmi_records.attachment_name LIKE '%.png'",
            "kanmi_records.attachment_name LIKE '%.gif'",
        ]
        let sqlWhereFilter = [];

        if (analyzerGroup && analyzerGroup.query) {
            sqlWhereFilter.push(analyzerGroup.query)
        } else {
            if (analyzerGroup && analyzerGroup.channels) {
                sqlWhereFilter.push('(' + analyzerGroup.channels.map(h => `kanmi_records.channel = '${h}'`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.servers) {
                sqlWhereFilter.push('(' + analyzerGroup.servers.map(h => `kanmi_records.server = '${h}'`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.content) {
                sqlWhereFilter.push('(' + analyzerGroup.content.map(h => `kanmi_records.content_full LIKE '%${h}%'`).join(' OR ') + ')');
            }

            if (analyzerGroup && analyzerGroup.parent) {
                sqlWhereFilter.push('(' + analyzerGroup.parent.map(h => `kanmi_channels.parent = '${h}'`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.class) {
                sqlWhereFilter.push('(' + analyzerGroup.class.map(h => `kanmi_channels.classification = '${h}'`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.vcid) {
                sqlWhereFilter.push('(' + analyzerGroup.vcid.map(h => `kanmi_channels.virtual_cid = '${h}'`).join(' OR ') + ')');
            }
        }

        const sqlOrderBy = (analyzerGroup && analyzerGroup.order) ? analyzerGroup.order :'eid DESC'
        const query = `SELECT x.*, y.host, y.path_hint, y.preview_hint, y.full_hint, y.mfull_hint FROM (SELECT ${sqlFields.join(', ')} FROM ${sqlTables.join(', ')} WHERE (${sqlWhereBase.join(' AND ')} AND (${sqlWhereFiletypes.join(' OR ')}))${(sqlWhereFilter.length > 0) ? ' AND (' + sqlWhereFilter.join(' AND ') + ')' : ''} ORDER BY ${sqlOrderBy} LIMIT ${(analyzerGroup && analyzerGroup.limit) ? analyzerGroup.limit : 100}) x LEFT JOIN (SELECT eid, host, path_hint, preview_hint, full_hint, mfull_hint FROM kanmi_records_cdn WHERE host = ${systemglobal.cdn_id} AND (preview_hint IS NOT NULL OR full_hint IS NOT NULL OR mfull_hint IS NOT NULL)) y ON (x.eid = y.eid)`
        customLogger('log', `Selecting data for analyzer group...`, analyzerGroup);

        const messages = (await sqlPromiseSafe(query, true)).rows.map(e => {
            let url
            if (e.preview_hint) {
                url = `${systemglobal.cdn_access_url}preview/${e.path_hint}/${e.preview_hint}`;
            } else if (e.full_hint) {
                url = `${systemglobal.cdn_access_url}full/${e.path_hint}/${e.full_hint}`;
            } else if (e.mfull_hint) {
                url = `${systemglobal.cdn_access_url}master/${e.path_hint}/${e.mfull_hint}`;
            } else if (e.attachment_auth && e.attachment_auth_valid === 1) {
                url = `https://cdn.discordapp.com/attachments/` + ((e.attachment_hash.includes('/')) ? e.attachment_hash : `${e.channel}/${e.attachment_hash}/${e.attachment_name.split('?')[0]}`) + `?${e.attachment_auth}`;
            } else {
                customLogger('log', 'Did not get any valid data to make a URL', e)
            }
            return { url, ...e };
        })
        customLogger('log', messages.length + ' items need to be tagged!')
        let downlaods = {}
        const existingFiles = [
            ...new Set([
                    ...((systemglobal.holding_path) ? fs.readdirSync(systemglobal.holding_path).map(e => e.split('.')[0]) : []),
                ...fs.readdirSync(systemglobal.deepbooru_input_path).map(e => e.split('.')[0]),
                ...fs.readdirSync(systemglobal.deepbooru_output_path).map(e => e.split('.')[0])
            ])
        ]
        messages.filter(e => !!e.url && existingFiles.indexOf(e.eid.toString()) === -1).map((e,i) => { downlaods[i] = e });
        if (messages.length === 0)
            return true;
        while (Object.keys(downlaods).length !== 0) {
            let downloadKeys = Object.keys(downlaods).slice(0,systemglobal.parallel_downloads || 25)
            customLogger('log', `${Object.keys(downlaods).length} Left to download`)
            await Promise.all(downloadKeys.map(async k => {
                const e = downlaods[k];
                const results = await new Promise(ok => {

                    const url = e.url;
                    request.get({
                        url,
                        headers: {
                            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                            'accept-language': 'en-US,en;q=0.9',
                            'cache-control': 'max-age=0',
                            'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-fetch-dest': 'document',
                            'sec-fetch-mode': 'navigate',
                            'sec-fetch-site': 'none',
                            'sec-fetch-user': '?1',
                            'upgrade-insecure-requests': '1',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.73'
                        },
                    }, async function (err, res, body) {
                        if (err) {
                            customLogger('error', `Download failed: ${url}`, err);
                            customLogger('error', e);
                            ok(false)
                        } else {
                            try {
                                if (body && body.length > 5000) {
                                    const mime = await new Promise(ext => {
                                        fileType.fromBuffer(body,function(err, result) {
                                            if (err) {
                                                customLogger('error', `Failed to get MIME type for ${e.eid}`, err);
                                                ext(null);
                                            } else {
                                                ext(result);
                                            }
                                        });
                                    })
                                    if (systemglobal.allow_direct_write && mime && mime.ext && ['png', 'jpg'].indexOf(mime.ext) !== -1) {
                                        fs.writeFileSync(path.join(systemglobal.holding_path || systemglobal.deepbooru_input_path, `${e.eid}.${mime.ext}`), body);
                                        ok(true);
                                    } else if ((!systemglobal.allow_direct_write && mime && mime.ext && ['png', 'jpg', 'gif', 'tiff', 'webp'].indexOf(mime.ext) !== -1) || (mime && mime.ext && ['gif', 'tiff', 'webp'].indexOf(mime.ext) !== -1)) {
                                        await sharp(body)
                                            .toFormat('png')
                                            .toFile(path.join(systemglobal.holding_path || systemglobal.deepbooru_input_path, `query-${e.eid}.png`), (err, info) => {
                                                if (err) {
                                                    customLogger('error', `Failed to convert ${e.eid} to PNG file`, err);
                                                    ok(false);
                                                } else {
                                                    //customLogger('log', `Downloaded as PNG ${e.url}`)
                                                    ok(true);
                                                }
                                            })
                                    } else {
                                        customLogger('error', 'Unsupported file, will be discarded! Please consider correcting file name');
                                        ok(false);
                                    }
                                } else {
                                    customLogger('error', `Download failed, File size to small: ${url}`);
                                    const eidData = (await sqlPromiseSafe(`SELECT * FROM kanmi_records WHERE id = ?`, [e.id])).rows
                                    if (eidData.length > 0) {
                                        mqClient.cdnRequest({
                                            messageIntent: "Reload",
                                            messageData: {
                                                ...eidData[0]
                                            },
                                            reCache: true
                                        })
                                    }
                                    ok(false);
                                }
                            } catch (err) {
                                customLogger('error', `Unexpected Error processing ${e.eid}!`, err);
                                customLogger('error', e)
                                ok(false);
                            }
                        }
                        delete downlaods[k];
                    })
                })
                if (!results) {
                    if (activeFiles.has(e.eid)) {
                        let prev = activeFiles.get(e.eid)
                        if (prev <= 5) {
                            prev++;
                            activeFiles.set(e.eid, prev);
                        } else {
                            await sqlPromiseSafe(`UPDATE kanmi_records SET tags = ? WHERE eid = ?`, [ '3/1/cant_tag; ', e.eid ]);
                            customLogger('error', `Failed to get data for ${e.eid} multiple times, it will be permanently skipped!`)
                        }
                    } else {
                        activeFiles.set(e.eid, 0);
                    }
                }
                return results;
            }))
        }
        return false;
    }
    async function parseResultsForMessage(key, results, bypass) {
        if (key) {
            return await new Promise(ok => {
                LocalQueue.getItem(key)
                    .then(data => {
                        if (data.message) {
                            if (results) {
                                const tags = Object.keys(results);
                                const rules = ruleSets.get(data.message.messageChannelID);
                                if (systemglobal.debug) {
                                    customLogger('log', `=== Evaluate Message ===`)
                                    customLogger('log', `TEXT: ${data.message.messageText}`)
                                    customLogger('log', `TAGS: ${tags}`)
                                    customLogger('log', `RULES: ${JSON.stringify(rules)}`)
                                    customLogger('log', `========================`)
                                }
                                const tagMatchesRule = (t, rule) => {
                                    if (rule.startsWith('s:')) {
                                        return t.startsWith(rule.slice(2));
                                    } else if (rule.startsWith('e:')) {
                                        return t.endsWith(rule.slice(2));
                                    } else {
                                        return rule === t;
                                    }
                                };
                                const result = (() => {
                                    if (rules && rules.block && tags.some(t => {
                                        return rules.block.some(rule => {
                                            if (rule.startsWith('s:')) {
                                                // Block if the tag starts with the specified string after 's:'
                                                return t.startsWith(rule.slice(2));
                                            } else if (rule.startsWith('e:')) {
                                                // Block if the tag ends with the specified string after 'e:'
                                                return t.endsWith(rule.slice(2));
                                            } else {
                                                // Regular match (exact match)
                                                return rule === t;
                                            }
                                        });
                                    })) {
                                        customLogger('error', `Found blocked tags "${tags.filter(t => rules.block.some(rule => {
                                            if (rule.startsWith('s:')) {
                                                return t.startsWith(rule.slice(2));
                                            } else if (rule.startsWith('e:')) {
                                                return t.endsWith(rule.slice(2));
                                            } else {
                                                return rule === t;
                                            }
                                        })).join(' ')}"`);
                                        return false;
                                    }
                                    if (rules && rules.block_pairs && rules.block_pairs.map(ph => ph.map(p => tags.filter(t => (p.indexOf(t) !== -1)).length).filter(g => !g).length).filter(h => h === 0).length > 0) {
                                        customLogger('error', `Found a blocked pair of tags "${rules.block_pairs.join(' + ')}"`)
                                        return false;
                                    }
                                    if (rules && rules.min_count && tags.length < rules.min_count) {
                                        customLogger('error', `Did not find enough tags (${tags.length})`)
                                        return false;
                                    }
                                    if (rules && rules.max_count && tags.length >= rules.max_count) {
                                        customLogger('error', `Returned to many tags (${tags.length}): Possible tag flood`)
                                        return false;
                                    }
                                    if (rules && rules.accept && tags.filter(t => {
                                        return rules.accept.some(rule => {
                                            if (rule.startsWith('s:')) {
                                                // Check if the tag starts with the specified string after 's:'
                                                return t.startsWith(rule.slice(2));
                                            } else if (rule.startsWith('e:')) {
                                                // Check if the tag ends with the specified string after 'e:'
                                                return t.endsWith(rule.slice(2));
                                            } else {
                                                // Regular match (exact match)
                                                return rule === t;
                                            }
                                        });
                                    }).length === 0) {
                                        customLogger('error', `Did not find approved tags "${tags}"`);
                                        return false;
                                    }
                                    return true;
                                })()
                                const folderMatch = (() => {
                                    if (rules && Array.isArray(rules.folder_match)) {
                                        for (let folderRule of rules.folder_match) {
                                            // Check min and max count
                                            if (folderRule.min_count && tags.length < folderRule.min_count) {
                                                continue; // Skip to the next folderRule if it doesn't meet the min_count
                                            }
                                            if (folderRule.max_count && tags.length >= folderRule.max_count) {
                                                continue; // Skip to the next folderRule if it exceeds the max_count
                                            }

                                            // Check for matching accepted tags
                                            if (folderRule.accept && tags.some(t => {
                                                return folderRule.accept.some(rule => tagMatchesRule(t, rule));
                                            })) {
                                                customLogger('error', `Found tag match for folder ${folderRule.destination}`);
                                                return folderRule.destination;
                                            }

                                            // Check for matching accepted tag pairs
                                            if (folderRule.accept_pairs && folderRule.accept_pairs.some(pair => {
                                                return pair.every(p => tags.some(t => tagMatchesRule(t, p)));
                                            })) {
                                                customLogger('error', `Found tag pair match for folder ${folderRule.destination}`);
                                                return folderRule.destination;
                                            }

                                            // Check for matching accepted tag pairs
                                            if (folderRule.text && data.message.messageText && folderRule.text.some(txt => data.message.messageText.toLowerCase().includes(txt.toLowerCase()))) {
                                                customLogger('error', `Found text string match for folder ${folderRule.destination}`);
                                                return folderRule.destination;
                                            }
                                        }
                                    }
                                    return undefined; // No matches found, return false
                                })();
                                if (folderMatch)
                                    customLogger('log', 'Adding to folder: ' + folderMatch);

                                const channelMatch = (() => {
                                    if (rules && Array.isArray(rules.channel_match)) {
                                        for (let channelRule of rules.channel_match) {
                                            // Check min and max count
                                            if (channelRule.min_count && tags.length < channelRule.min_count) {
                                                continue; // Skip to the next folderRule if it doesn't meet the min_count
                                            }
                                            if (channelRule.max_count && tags.length >= channelRule.max_count) {
                                                continue; // Skip to the next folderRule if it exceeds the max_count
                                            }

                                            // Check for matching accepted tags
                                            if (channelRule.accept && tags.some(t => {
                                                return channelRule.accept.some(rule => tagMatchesRule(t, rule));
                                            })) {
                                                customLogger('error', `Found tag match for folder ${folderRule.destination}`);
                                                return channelRule.destination;
                                            }

                                            // Check for matching accepted tag pairs
                                            if (channelRule.accept_pairs && channelRule.accept_pairs.some(pair => {
                                                return pair.every(p => tags.some(t => tagMatchesRule(t, p)));
                                            })) {
                                                customLogger('error', `Found tag pair match for folder ${channelRule.destination}`);
                                                return channelRule.destination;
                                            }

                                            // Check for matching accepted tag pairs
                                            if (channelRule.text && data.message.messageText && channelRule.text.some(txt => data.message.messageText.toLowerCase().includes(txt.toLowerCase()))) {
                                                customLogger('error', `Found text string match for folder ${channelRule.destination}`);
                                                return channelRule.destination;
                                            }
                                        }
                                    }
                                    return undefined; // No matches found, return false
                                })();
                                if (channelMatch)
                                    customLogger('log', 'Redirecting to channel: ' + channelMatch);

                                let tagString = (Object.keys(results).map(k => `${modelTags.get(k) || 0}/${parseFloat(results[k]).toFixed(4)}/${k}`).join('; ') + '; ')
                                if (result) {
                                    ok({
                                        destination: `${systemglobal.mq_discord_out}${(data.queue !== 'normal') ? '.' + data.queue : ''}`,
                                        message: {
                                            fromDPS: `return.${facilityName}.${systemglobal.system_name}`,
                                            ...data.message,
                                            messageChannelID: channelMatch || data.messageChannelID,
                                            messageTags: tagString,
                                            messageChannelFolder: folderMatch
                                        }
                                    });
                                } else {
                                    ok(false)
                                }
                            } else if (bypass) {
                                customLogger('error', `Bypassing ${key}, No tags provided`);
                                ok({
                                    destination: `${systemglobal.mq_discord_out}${(data.queue !== 'normal') ? '.' + data.queue : ''}`,
                                    message: {
                                        fromDPS: `return.${facilityName}.${systemglobal.system_name}`,
                                        ...data.message
                                    }
                                });
                            } else {
                                ok(false)
                            }

                        } else {
                            customLogger('error', `Unexpectedly Failed to get message data for key ${key}`)
                            ok(false);
                        }
                    })
                    .catch(err => {
                        customLogger('error', `Unexpectedly Failed to get message for key ${key}`, err)
                        ok(false);
                    })
            })
        }
        return false;
    }
    async function parseResultsForQuery(channel, results) {
        if (channel) {
            return await new Promise(ok => {
                if (results) {
                    const tags = Object.keys(results);
                    const rules = ruleSets.get(channel);
                    const tagMatchesRule = (t, rule) => {
                        if (rule.startsWith('s:')) {
                            return t.startsWith(rule.slice(2));
                        } else if (rule.startsWith('e:')) {
                            return t.endsWith(rule.slice(2));
                        } else {
                            return rule === t;
                        }
                    };
                    const result = (() => {
                        if (rules && rules.block && tags.some(t => {
                            return rules.block.some(rule => {
                                if (rule.startsWith('s:')) {
                                    // Block if the tag starts with the specified string after 's:'
                                    return t.startsWith(rule.slice(2));
                                } else if (rule.startsWith('e:')) {
                                    // Block if the tag ends with the specified string after 'e:'
                                    return t.endsWith(rule.slice(2));
                                } else {
                                    // Regular match (exact match)
                                    return rule === t;
                                }
                            });
                        })) {
                            customLogger('error', `Found blocked tags "${tags.filter(t => rules.block.some(rule => {
                                if (rule.startsWith('s:')) {
                                    return t.startsWith(rule.slice(2));
                                } else if (rule.startsWith('e:')) {
                                    return t.endsWith(rule.slice(2));
                                } else {
                                    return rule === t;
                                }
                            })).join(' ')}"`);
                            return false;
                        }
                        if (rules && rules.block_pairs && rules.block_pairs.map(ph => ph.map(p => tags.filter(t => (p.indexOf(t) !== -1)).length).filter(g => !g).length).filter(h => h === 0).length > 0) {
                            customLogger('error', `Found a blocked pair of tags "${rules.block_pairs.join(' + ')}"`)
                            return false;
                        }
                        if (rules && rules.min_count && tags.length < rules.min_count) {
                            customLogger('error', `Did not find enough tags (${tags.length})`)
                            return false;
                        }
                        if (rules && rules.max_count && tags.length >= rules.max_count) {
                            customLogger('error', `Returned to many tags (${tags.length}): Possible tag flood`)
                            return false;
                        }
                        if (rules && rules.accept && tags.filter(t => {
                            return rules.accept.some(rule => {
                                if (rule.startsWith('s:')) {
                                    // Check if the tag starts with the specified string after 's:'
                                    return t.startsWith(rule.slice(2));
                                } else if (rule.startsWith('e:')) {
                                    // Check if the tag ends with the specified string after 'e:'
                                    return t.endsWith(rule.slice(2));
                                } else {
                                    // Regular match (exact match)
                                    return rule === t;
                                }
                            });
                        }).length === 0) {
                            customLogger('error', `Did not find approved tags "${tags}"`);
                            return false;
                        }
                        return true;
                    })()
                    const folderMatch = (() => {
                        if (rules && Array.isArray(rules.folder_match)) {
                            for (let folderRule of rules.folder_match) {
                                // Check min and max count
                                if (folderRule.min_count && tags.length < folderRule.min_count) {
                                    continue; // Skip to the next folderRule if it doesn't meet the min_count
                                }
                                if (folderRule.max_count && tags.length >= folderRule.max_count) {
                                    continue; // Skip to the next folderRule if it exceeds the max_count
                                }

                                // Check for matching accepted tags
                                if (folderRule.accept && tags.some(t => {
                                    return folderRule.accept.some(rule => tagMatchesRule(t, rule));
                                })) {
                                    customLogger('error', `Found tag match for folder ${folderRule.destination}`);
                                    return folderRule.destination;
                                }

                                // Check for matching accepted tag pairs
                                if (folderRule.accept_pairs && folderRule.accept_pairs.some(pair => {
                                    return pair.every(p => tags.some(t => tagMatchesRule(t, p)));
                                })) {
                                    customLogger('error', `Found tag pair match for folder ${folderRule.destination}`);
                                    return folderRule.destination;
                                }
                            }
                        }

                        return undefined; // No matches found, return false
                    })();
                    if (folderMatch)
                        customLogger('log', 'Adding to folder: ' + folderMatch);
                    ok({
                        approval: result,
                        folder: folderMatch
                    });
                } else {
                    ok({
                        approval: true
                    })
                }
            })
        }
        return {
            approval: true
        };
    }
    async function validateImageInputs() {
        customLogger('log', "Validating Image Inputs...");

        // Check if holding_path is set
        if (systemglobal.holding_path) {
            const inputFiles = fs.readdirSync(systemglobal.deepbooru_input_path);
            if (inputFiles.length > 0) {
                customLogger('log', `${inputFiles.length} files are pending! Skipping loading of images.`);
            } else {
                const holdingFiles = fs.readdirSync(systemglobal.holding_path);
                if (holdingFiles.length > (systemglobal.max_load || 150)) {
                    customLogger('log', `There are ${holdingFiles.length} files pending. Processing first ${(systemglobal.max_load || 150)} files.`);
                }

                // Move up to 150 files to the deepbooru_input_path
                const filesToMove = holdingFiles.slice(0, (systemglobal.max_load || 150));
                for (let file of filesToMove) {
                    fs.renameSync(path.join(systemglobal.holding_path, file), path.join(systemglobal.deepbooru_input_path, file));
                }
            }
        }

        const imageFile = fs.readdirSync(systemglobal.deepbooru_input_path)
        for (let e of imageFile) {
            try {
                let image = await sharp(fs.readFileSync(path.join(systemglobal.deepbooru_input_path, e))).metadata();
                if (image.format === 'png' && parsedImages.indexOf(e) === -1) {
                    // Always convert the image to PNG
                    await sharp(fs.readFileSync(path.join(systemglobal.deepbooru_input_path, e)))
                        .png() // Convert to PNG
                        .toFile(path.join(systemglobal.deepbooru_input_path, e));
                    parsedImages.push(e);
                }
                image = null;
            } catch (err) {
                if (warnedImages[e] !== undefined && warnedImages[e] < totalItems) {
                    customLogger('error', `Image is invalid: ${e}`, err.message);
                    try {
                        if (e.includes('message-')) {
                            const key = e.split('message-').pop().split('.')[0];
                            const a = await parseResultsForMessage(key, undefined, true);
                            mqClient.sendData( `${a.destination}`, a.message, function (ok) { });
                        }
                        fs.unlinkSync(path.join(systemglobal.deepbooru_input_path, e));
                    } catch (e) {
                        customLogger('error', err.message);
                    }
                } else {
                    warnedImages[e] = totalItems;
                    customLogger('log', `Image may be invalid (Marked for catch): ${e}`, err.message);
                }
            }
        }
        fs.writeFileSync(path.join('./', 'watchedFiles.json'), JSON.stringify({ catchList: warnedImages}))
    }

    async function parseUntilDone(analyzerGroups, skipSearch) {
        while (true) {
            let noResults = 0;
            if (analyzerGroups) {
                await new Promise(completed => {
                    let requests = analyzerGroups.reduce((promiseChain, w) => {
                        return promiseChain.then(() => new Promise(async (resolve) => {
                            const _r = await queryForTags(w);
                            if (_r)
                                noResults++;
                            resolve(true);
                        }))
                    }, Promise.resolve());
                    requests.then(async () => {
                        if (noResults !== analyzerGroups.length) {
                            customLogger('log', 'Search Jobs Completed!, Starting MIITS Tagger...');
                            if (!gpuLocked && startEvaluating === null) {
                                clearTimeout(startEvaluating);
                                startEvaluating = null;
                                startEvaluating = setTimeout(processGPUWorkloads, 3000)
                            }
                            await sleep(5000);
                            while (mittsIsActive) {
                                await sleep(5000);
                            }
                            customLogger('log', 'MIITS Tagger finished!');
                            completed();
                        } else {
                            completed();
                        }
                    })
                })
            } else {
                if (!skipSearch) {
                    const _r = await queryForTags();
                    if (_r)
                        noResults++;
                    customLogger('log', 'Search Jobs Completed!, Starting MIITS Tagger...');
                } else {
                    const existingFiles = [
                        ...new Set([
                            ...((systemglobal.holding_path) ? fs.readdirSync(systemglobal.holding_path).map(e => e.split('.')[0]) : []),
                            ...fs.readdirSync(systemglobal.deepbooru_input_path).map(e => e.split('.')[0]),
                            ...fs.readdirSync(systemglobal.deepbooru_output_path).map(e => e.split('.')[0])
                        ])
                    ]
                    if (existingFiles.length === 0)
                        noResults++;
                    customLogger('log', 'Starting MIITS Tagger...');
                }
                clearTimeout(startEvaluating);
                startEvaluating = null;
                startEvaluating = setTimeout(processGPUWorkloads, 3000)
                await sleep(5000);
                while (mittsIsActive) {
                    await sleep(5000);
                }
                customLogger('log', 'MIITS Tagger finished!');
            }
            if ((analyzerGroups && noResults === analyzerGroups.length) || (!analyzerGroups && noResults === 1))
                break;
            customLogger('log', 'More work to be done, waiting for sync...!');
            await new Promise(done => setTimeout(() => {
                checkMemoryUsage();
                done(true);
            }, 60000))
        }
        customLogger('log', 'Waiting for next run... Zzzzz')
        runTimer = setTimeout(parseUntilDone, 300000);
    }

    process.on('uncaughtException', async (err) => {
        customLogger('log', err);
    })
})()
