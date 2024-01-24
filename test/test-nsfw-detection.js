const express = require("express");
const path = require("path");
(async () => {
    const tf = require('@tensorflow/tfjs-node');
    const nsfw = require('nsfwjs');
    const sharp = require('sharp');
    const fs = require('fs');
    const express = require('express');
    const jpeg = require('jpeg-js');

    const app = express();

    const convert = async (img) => {
        const rawImageData = await sharp(img).raw().jpeg().toBuffer()
        const decoded = jpeg.decode(rawImageData);
        const { width, height, data } = decoded
        const buffer = new Uint8Array(width * height * 3);
        let offset = 0;
        for (let i = 0; i < buffer.length; i += 3) {
            buffer[i] = data[offset];
            buffer[i + 1] = data[offset + 1];
            buffer[i + 2] = data[offset + 2];

            offset += 4;
        }
        return tf.tensor3d(buffer, [height, width, 3]);
    }
    app.use(express.static(path.join('../utils/models/')));
    const server = app.listen(9052, async function(err) {
        if (err) {
            console.log('App listening error ', err);
        } else {
            console.log('App running at 9052')
        }

        await tf.enableProdMode();
        await tf.ready();

        const model = await nsfw.load(`http://localhost:9052/nsfw/`, { size: 224 });
        const files = fs.readdirSync('demo-images/');
        for (const file of files) {
            const img = await convert(`demo-images/${file}`);
            const predictions = await model.classify(img);
            const threshold = 0.5;
            const filteredPredictions = predictions.filter(prediction => prediction.probability > threshold);
            console.log(file, filteredPredictions[0].className, predictions);
        }
    });
})()
