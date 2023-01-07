import fs from "fs";
import config from "./config.json";
import path from "path";

(async () => {
    const config = require('./config.json');
    const md5 = require('md5');
    const cron = require('node-cron');
    const { spawn, exec } = require("child_process");
    const fs = require('fs');
    const path = require('path');
    const request = require('request').defaults({ encoding: null, jar: true });
    const {sqlPromiseSafe, sqlPromiseSimple} = require("./utils/sqlClient");
    const {sendData} = require("./utils/mqAccess");
    const Discord_CDN_Accepted_Files = ['jpg','jpeg','jfif','png','webp','gif'];

    console.log("Reading tags from database...");
    let exsitingTags = new Map();
    (await sqlPromiseSafe(`SELECT id, name FROM sequenzia_index_tags`)).rows.map(e => exsitingTags.set(e.name, e.id));
    console.log("Reading tags from model...");
    let modelTags = new Map();
    const _modelTags = (fs.readFileSync(path.join(config.deepbooru_model_path, './tags.txt'))).toString().trim().split('\n').map(line => line.trim());
    const _modelCategories = JSON.parse(fs.readFileSync(path.join(config.deepbooru_model_path, './categories.json')).toString());
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
    console.log(`Loaded ${modelTags.size} tags from model`);


    async function clearFolder(folderPath) {
        try {
            const files = await fs.promises.readdir(folderPath);
            for (const file of files) {
                await fs.promises.unlink(path.resolve(folderPath, file));
                console.log(`${folderPath}/${file} has been removed successfully`);
            }
        } catch (err){
            console.log(err);
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
    function updateTagsPairs(eid, tags) {
        return tags.map(async tag => await addTagForEid(eid, tag.name, tag.rating))
    }
    async function queryImageTags() {
        console.log('Processing images for tags...')
        return new Promise(async (resolve) => {
            const startTime = Date.now()
            let ddOptions = ['evaluate', config.deepbooru_input_path, '--project-path', config.deepbooru_model_path, '--allow-folder', '--save-json', '--save-path', config.deepbooru_output_path, '--no-tag-output']
            if (config.deepbooru_gpu)
                ddOptions.push('--allow-gpu')
            console.log(ddOptions.join(' '))
            await clearFolder(config.deepbooru_input_path);
            const muginoMeltdown = spawn(((config.deepbooru_exec) ? config.deepbooru_exec : 'deepbooru'), ddOptions, { encoding: 'utf8' })

            muginoMeltdown.stdout.on('data', (data) => console.log(data.toString()))
            muginoMeltdown.stderr.on('data', (data) => console.error(data.toString()));
            muginoMeltdown.on('close', (code, signal) => {
                if (code !== 0) {
                    console.error(`Mugino Meltdown! MIITS reported a error!`);
                    resolve(false)
                } else {
                    console.log(`Tagging Completed in ${((startTime - Date.now()) / 1000).toFixed(0)} sec!`);
                    resolve(true)
                }
            })
        })
    }
    async function queryForTags() {
        const messages = (await sqlPromiseSafe(`SELECT attachment_name, channel, attachment_hash, eid, cache_proxy, sizeH, sizeW
                                                FROM kanmi_records
                                                WHERE attachment_hash IS NOT NULL
                                                  AND channel = ?
                                                  AND eid NOT IN (SELECT eid FROM sequenzia_index_matches)
                                                ORDER BY eid DESC
                                                LIMIT 10`, [config.channel], true)).rows.map(e => {
            const url = (( e.cache_proxy) ? e.cache_proxy.startsWith('http') ? e.cache_proxy : `https://media.discordapp.net/attachments${e.cache_proxy}` : (e.attachment_hash && e.attachment_name) ? `https://media.discordapp.net/attachments/` + ((e.attachment_hash.includes('/')) ? e.attachment_hash : `${e.channel}/${e.attachment_hash}/${e.attachment_name}`) : undefined) + '';
            return {
                url,
                ...e
            }
        })
        console.log(messages.length)
        let msgRequests = messages.reduce((promiseChain, e, i, a) => {
            return promiseChain.then(() => new Promise(async(completed) => {
                const fileExt = e.url.split('.').pop();
                completed(await new Promise(ok => {
                    function getimageSizeParam() {
                        if (e.sizeH && e.sizeW && Discord_CDN_Accepted_Files.indexOf(e.attachment_name.split('.').pop().toLowerCase()) !== -1 && (e.sizeH > 512 || e.sizeW > 512)) {
                            let ih = 512;
                            let iw = 512;
                            if (e.sizeW >= e.sizeH) {
                                iw = (e.sizeW * (512 / e.sizeH)).toFixed(0)
                            } else {
                                ih = (e.sizeH * (512 / e.sizeW)).toFixed(0)
                            }
                            return `?width=${iw}&height=${ih}`
                        } else {
                            return ''
                        }
                    }
                    request.get({
                        url: e.url + getimageSizeParam(),
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
                    }, function (err, res, body) {
                        if (err) {
                            ok(null)
                        } else {
                            try {
                                fs.writeFileSync(path.join(config.deepbooru_input_path, `${e.eid}.${fileExt}`), body);
                                ok(true);
                            } catch (err) {
                                console.error(err);
                                ok(false);
                            }
                        }
                    })
                }));
            }))
        }, Promise.resolve());
        msgRequests.then(async () => {
            console.log('Completed Image Download!');
            await queryImageTags();
            let imageTagRequests = messages.reduce((promiseChain, e, i, a) => {
                return promiseChain.then(() => new Promise(async(completed) => {
                    const jsonFilePath = path.join(config.deepbooru_output_path, `${e.eid}.json`)
                    if (fs.existsSync(jsonFilePath)) {
                        const tagResults = JSON.parse(fs.readFileSync(jsonFilePath).toString());
                        console.error(`Entity ${e.eid} has ${Object.keys(tagResults).length} tags!`);
                        Object.keys(tagResults).map(async k => {
                            const r = tagResults[k];
                            await addTagForEid(e.eid, k, r);
                        });
                        completed(true);
                    } else {
                        console.error(`Entity ${e.eid} has no resulting JSON file! Maybe something went wrong withs MIITS`);
                        completed(false);
                    }
                }))
            }, Promise.resolve());
            msgRequests.then(async () => {
                console.log('Completed Image Tagging!');
            });
        })
    }
    //cron.schedule('45 * * * *', async () => { queryForTags(); });
    await queryForTags();
})()
