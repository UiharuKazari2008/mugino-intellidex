const fs = require('fs');
const path = require('path');

// Path to the directory
const directoryPath = path.join('data', 'LocalQueue');

// Function to process JSON files and remove 'itemFileData' property
const processFilesSync = () => {
    try {
        const files = fs.readdirSync(directoryPath);

        // Loop through all files
        files.forEach((file) => {
            const filePath = path.join(directoryPath, file);

            try {
                // Read the JSON file
                const data = fs.readFileSync(filePath, 'utf8');

                // Parse the JSON content
                let jsonData = JSON.parse(data);

                delete jsonData.value.message.itemFileData;

                // Convert back to JSON and write it to the file
                fs.writeFileSync(filePath, JSON.stringify(jsonData, null, 2));
                console.log('Updated file:', file);
            } catch (err) {
                console.log('Error processing file:', file, err);
            }
        });
    } catch (err) {
        console.log('Unable to scan directory:', err);
    }
};

// Start processing the files
processFilesSync();
