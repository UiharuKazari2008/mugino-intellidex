const fs = require('fs');
const path = require('path');

// Path to the directory
const directoryPath = path.join(__dirname, 'data', 'LocalQueue');

// Function to process JSON files and remove 'itemFileData' property
const processFiles = () => {
    fs.readdir(directoryPath, (err, files) => {
        if (err) {
            return console.log('Unable to scan directory: ' + err);
        }

        // Loop through all files
        files.forEach((file) => {
            const filePath = path.join(directoryPath, file);

            fs.readFile(filePath, 'utf8', (err, data) => {
                if (err) {
                    console.log('Error reading file:', file, err);
                    return;
                }

                try {
                    // Parse the JSON content
                    let jsonData = JSON.parse(data);

                    // Remove 'itemFileData' property if it exists
                    if (jsonData.hasOwnProperty('itemFileData')) {
                        delete jsonData.itemFileData;

                        // Convert back to JSON and write it to the file
                        fs.writeFile(filePath, JSON.stringify(jsonData, null, 2), (err) => {
                            if (err) {
                                console.log('Error writing file:', file, err);
                            } else {
                                console.log('Updated file:', file);
                            }
                        });
                    }
                } catch (jsonErr) {
                    console.log('Error parsing JSON file:', file, jsonErr);
                }
            });
        });
    });
};

// Start processing the files
processFiles();
