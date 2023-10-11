import { Worker, isMainThread, parentPort } from "worker_threads";

import { Logger } from "./types";
import { deDuplicate, filterValidSources } from "./helpers"
import { Database } from "./database";

/*
    Here we execute the main logic of the app,
    we retrieve the all records sorted in descending order by the length
    of the titles then we eliminate the duplicates and extract the sources that
    are most frequently associated with a given title and subsequently mark them valid
*/
async function commenceOperation(database: Database) {

    const sortedByTitle = await database.getSortedByTitleRecords();
    let validRows = filterValidSources(deDuplicate(sortedByTitle));
    console.log("de-duplication done");


    await database.addIsValidColumn();

    for(let validRow of validRows) {
        await database.setIsValidRecord(validRow.ids);
    }

    await database.closeConnection();

    console.log("de-duplication and updating is_valid column done");

}

/*
    to enable parallel processing of records and logging we spawn a web worker, where
    the main logic of the app is within the primary thread and logging/updating the 
    system_status table is delegated to a child thread.
*/

async function main() {

    const database = new Database();
    await database.connect();

    if(isMainThread) {

        console.log("in main thread");

        const childWorker = new Worker(__filename, {});
        childWorker.on("message", (msg)=> {

            if(msg.status === Logger.LOGGER_ONLINE ) {
                console.log("Logger is online now");
            } else if(msg.status === Logger.LOGGER_OFFLINE) {
                database.closeConnection();
                console.log("Logger is offline now");
            }
        });
        childWorker.postMessage({ loggerStatus: Logger.ONLINE });


        await commenceOperation(database);


        childWorker.postMessage({ loggerStatus: Logger.OFFLINE });
        console.log("main thread has ended");
        
    } else {

        let intervalHandler:any;

        parentPort!.on('message', async (message)=> {

            if(message.loggerStatus === Logger.ONLINE) {
                
                parentPort?.postMessage({ status : Logger.LOGGER_ONLINE})
                intervalHandler = setInterval( ()=> {
                    database.setStatus();
                }, 500);

            }else if(message.loggerStatus === Logger.OFFLINE) {
                
                if(intervalHandler) {
                    clearInterval(intervalHandler);
                    parentPort?.postMessage({ status : Logger.LOGGER_OFFLINE});
                }
            }
        });
    }
}

main();