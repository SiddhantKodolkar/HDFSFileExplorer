const axios = require('axios');
const readline = require('readline');
const fs = require('fs');
const FormData = require('form-data');
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

// Function to get the status of a file or directory in HDFS
// Function to get the status of a file or directory in HDFS
async function getFileStatus() {
    try {
        // Make a GET request to the WebHDFS API to list status of files in the root directory
        const response = await axios.get('http://localhost:9870/webhdfs/v1/?op=LISTSTATUS');

        // Extract the FileStatuses object from the response data
        const fileStatuses = response.data.FileStatuses.FileStatus;

        // Output the list of files and directories
        console.log("Files and Directories in Root:");
        console.log("-----------------------------");
        fileStatuses.forEach((fileStatus, index) => {
            console.log(`${index + 1}. ${fileStatus.pathSuffix}`);
        });
        console.log("\n");

        rl.question("Enter the index of the file to get status: ", async (index) => {
            const selectedFile = fileStatuses[index - 1];
            if (selectedFile) {
                const filePath = selectedFile.pathSuffix;
                try {
                    // Make a GET request to the WebHDFS API
                    const statusResponse = await axios.get(`http://localhost:9870/webhdfs/v1/${filePath}?op=GETFILESTATUS`);

                    // Extract the FileStatus object from the response data
                    const fileStatus = statusResponse.data.FileStatus;

                    // Output the file status information
                    console.log("File Status:");
                    console.log("------------");
                    console.log("Path: ", filePath);
                    console.log("Access Time: ", new Date(fileStatus.accessTime));
                    console.log("Block Size: ", fileStatus.blockSize);
                    console.log("Group: ", fileStatus.group);
                    console.log("Length: ", fileStatus.length);
                    console.log("Owner: ", fileStatus.owner);
                    console.log("Permission: ", fileStatus.permission);
                    console.log("Replication: ", fileStatus.replication);
                    console.log("Type: ", fileStatus.type);
                    console.log("\n");

                } catch (error) {
                    // Handle any errors
                    console.error("Error occurred while getting file status:", error);
                }
            } else {
                console.log("Invalid index.");
            }
            rl.close();
        });

    } catch (error) {
        // Handle any errors
        console.error("Error occurred while listing files in root directory:", error);
    }
}


// Function to list files and directories in the root directory of HDFS
async function listFilesInRoot() {
    try {
        // Make a GET request to the WebHDFS API to list status of files in the root directory
        const response = await axios.get('http://localhost:9870/webhdfs/v1/?op=LISTSTATUS');

        // Extract the FileStatuses object from the response data
        const fileStatuses = response.data.FileStatuses.FileStatus;

        // Output the list of files and directories
        console.log("Files and Directories in Root:");
        console.log("-----------------------------");
        fileStatuses.forEach(fileStatus => {
            console.log(fileStatus.pathSuffix);
        });
        console.log("\n");

    } catch (error) {
        // Handle any errors
        console.error("Error occurred while listing files in root directory:", error);
    }
}

// Function to create a directory in the root directory of HDFS
async function createDirectoryInRoot(directoryName) {
    try {
        // Make a PUT request to the WebHDFS API to create a directory
        const response = await axios.put(`http://localhost:9870/webhdfs/v1/${directoryName}?op=MKDIRS`);

        // Check if directory creation was successful
        if (response.status === 200) {
            console.log(`Directory '${directoryName}' created successfully in root directory of HDFS.`);
        } else {
            console.error(`Error creating directory '${directoryName}' in root directory of HDFS: ${response.statusText}`);
        }
    } catch (error) {
        // Handle any errors
        console.error("Error occurred while creating directory in root directory:", error);
    }
}

// Add this function to handle file download
async function downloadFile(fileName) {
    try {
        // Make a GET request to the WebHDFS API to open the file for reading
        const response = await axios.get(`http://localhost:9870/webhdfs/v1/${fileName}?op=OPEN`, {
            responseType: 'stream' // Set responseType to 'stream' to handle binary data
        });

        // Create a writable stream to save the file locally
        const fs = require('fs');
        const filePath = `./${fileName}`; // Local file path where the file will be saved
        const fileStream = fs.createWriteStream(filePath);

        // Pipe the response data (file content) to the writable stream
        response.data.pipe(fileStream);

        // Wait for the file to finish downloading
        await new Promise((resolve, reject) => {
            fileStream.on('finish', () => {
                console.log(`File '${fileName}' downloaded successfully.`);
                resolve();
            });
            fileStream.on('error', (error) => {
                console.error(`Error occurred while downloading file '${fileName}':`, error);
                reject(error);
            });
        });
    } catch (error) {
        console.error(`Error occurred while downloading file '${fileName}':`, error);
    }
}

// Update the downloadFileFromHDFS function to call the downloadFile function with the selected file name
async function downloadFileFromHDFS() {
    try {
        // Make a GET request to the WebHDFS API to list files
        const response = await axios.get('http://localhost:9870/webhdfs/v1/?op=LISTSTATUS');

        // Extract file names from the response
        const fileStatuses = response.data.FileStatuses.FileStatus;
        const fileNames = fileStatuses.map(file => file.pathSuffix);

        // Display file names to the user
        console.log("Files in HDFS:");
        fileNames.forEach((name, index) => {
            console.log(`${index + 1}. ${name}`);
        });

        // Prompt user to select a file
        rl.question("Enter the index of the file you want to download: ", async (index) => {
            const selectedFileName = fileNames[index - 1];
            if (selectedFileName) {
                await downloadFile(selectedFileName); // Call downloadFile function with selected file name
            } else {
                console.log("Invalid index.");
            }
            rl.close();
        });
    } catch (error) {
        console.error("Error occurred while listing files in HDFS:", error);
        rl.close();
    }
}

// Function to preview the content of a text file
async function previewFileFromHDFS(fileName) {
    try {
        // Make a GET request to the WebHDFS API to list files
        const response = await axios.get('http://localhost:9870/webhdfs/v1/?op=LISTSTATUS');

        // Extract file names from the response
        const fileStatuses = response.data.FileStatuses.FileStatus;
        const fileNames = fileStatuses.map(file => file.pathSuffix);

        // Display file names to the user
        console.log("Files in HDFS:");
        fileNames.forEach((name, index) => {
            console.log(`${index + 1}. ${name}`);
        });

        // Prompt user to select a file
        rl.question("Enter the index of the file you want to preview: ", async (index) => {
            const selectedFileName = fileNames[index - 1];
            if (selectedFileName) {
                const response = await axios.get(`http://localhost:9870/webhdfs/v1/${selectedFileName}?op=OPEN`);
                
                // Read the content of the file
                const fileContent = response.data;
                
                // Output the file content to the console
                console.log("File Preview:");
                console.log("-------------");
                console.log(fileContent);
                console.log("\n");
            } else {
                console.log("Invalid index.");
            }
            rl.close();
        });
    } catch (error) {
        console.error("Error occurred while listing files in HDFS:", error);
        rl.close();
    }
}

// Function to determine which operation to perform based on user input
async function performOperation(operation) {
    switch (operation) {
        case '1':
            const filePath = "todo.txt"; // Specify the path of the file or directory in HDFS
            await getFileStatus(filePath);
            break;
        case '2':
            await listFilesInRoot();
            break;
        case '3':
            rl.question("Enter directory name: ", async (directoryName) => {
                await createDirectoryInRoot(directoryName);
                rl.close();
            });
            break;
        case '4':
            await downloadFileFromHDFS();
            break;
        case '5':
            // Prompt user to enter the name of the file to preview
            await previewFileFromHDFS();
            break;
        default:
            console.log("Invalid operation.");
    }
}

// Prompt user to select operation
console.log("Select operation:");
console.log("(1) Get file status");
console.log("(2) List files in root");
console.log("(3) Create directory in root");
console.log("(4) Download File from HDFS");
console.log("(5) Preview File");

rl.question("Enter your choice: ", (operation) => {
    performOperation(operation);
});

