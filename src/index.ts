import { Client } from "pg";
import { Worker, isMainThread, parentPort, workerData } from "worker_threads";
import * as dotenv from "dotenv";
import assert from "assert";

import { DeDuplicatedRow, ValidRow, Logger } from "./types";

async function getDBConnection() {

    dotenv.config();

    const client = new Client({
        user: process.env.DB_USER,
        host: process.env.DB_HOST,
        database: process.env.DB_NAME,
        password: process.env.DB_PASSWORD,
        port: 5432
    });

    return client;
}

function checkDuplicate(base: string, duplicate: string): boolean {

    assert.ok(base.length >= duplicate.length);
    
    if(base === duplicate) {
        return true;
    }

    const eightyPercent = base.length * (4/5);
    for(let i=0; i<base.length; i++) {
        
        if(i === duplicate.length && (i+1) >= eightyPercent) {
            return true;
        }

        if(base[i] !== duplicate[i]) {
            
            if((i+1) >= eightyPercent ) {
                return true
            }
            return false;
        }
    }

    return false;
}

function deDuplicate(rows: any) {

    let dedupRows: DeDuplicatedRow[] = [];

    while(rows.length > 0) {
        
        let baseRow = rows.shift();
        dedupRows.push({ 
            title : baseRow.title,  
            sources : {
                [baseRow.source_type] : [ baseRow.id ]
            } 
        });

        rows = rows.filter((row: any)=> {

            let lastDedupRow = dedupRows[dedupRows.length - 1];

            if(checkDuplicate(lastDedupRow.title, row.title)) {

                if(lastDedupRow.sources[row.source_type]) {

                    lastDedupRow.sources[row.source_type].push(row.id);

                } else {

                    lastDedupRow.sources[row.source_type] = [ row.id ];
                }
                return false;
            }
            return true;
        });
    }

    return dedupRows;

}

function filterValidSources(deDupRows: DeDuplicatedRow[]) {

    let validRows: ValidRow[] = [];

    validRows = deDupRows.map(row=> {
        
        let validRow = {} as ValidRow;

        let validSource = Object.keys(row.sources).reduce((validKey: string, key: string)=> {

            if(row.sources[key].length > row.sources[validKey].length) {
                return key;
            }
            return validKey;
        });

        validRow["title"] = row.title;
        validRow["ids"] = row.sources[validSource];

        return validRow;
    });

    return validRows;
}

async function main() {

    let connection = await getDBConnection();

    try {

        connection.connect();

        const getSortedTitlesQuery = "SELECT * FROM jobs_raw_copy ORDER BY LENGTH(title) DESC;";
        const alterTableQuery = "ALTER TABLE jobs_raw_copy ADD COLUMN is_valid BOOLEAN;";
        

        let result = await connection.query(getSortedTitlesQuery);
        
        let deDuplicatedRows = deDuplicate(result.rows);
        let validRows = filterValidSources(deDuplicatedRows);
        console.log("de duplication done");

        await connection.query(alterTableQuery);
        console.log("is_valid column added to raw-jobs");

        for(let validRow of validRows) {
            
            let idList = validRow.ids.map(id=>id.toString()).reduce((idList: string, id: string)=> {
                
                idList = idList + "," + id;
                return idList;
            });

            let updateIsValidQuery = `UPDATE jobs_raw_copy SET is_valid = TRUE WHERE id IN (${idList});`;
            await connection.query(updateIsValidQuery);
            
        }

        await connection.end();

        console.log("deduplication and updating is_valid column done");

    } catch(dbError: any) {
        console.log(dbError);
    }
}

//main();

async function foomain() {

    const startTime = Date.now();
    const timeout = 60000;

    if(isMainThread) {

        console.log("in main thread");

        const childWorker = new Worker(__filename, {});
        childWorker.on("message", (msg)=> {

            if(msg.status === Logger.LOGGER_ONLINE ) {
                
                console.log("Logger is online now");
            } else if(msg.status === Logger.LOGGER_ONLINE) {
                
                console.log("Logger is offline now");
            }
        });
        childWorker.postMessage({ loggerStatus: Logger.ONLINE });
        
        while (true) {
            if (Date.now() - startTime >= timeout) {
                console.log('Timeout exceeded');
                break;
            }
        }
        childWorker.postMessage({ loggerStatus: Logger.OFFLINE });
        console.log("main thread has ended");
        
    } else {

        let intervalHandler:any;
        const conn = await getDBConnection();

        parentPort!.on('message', async (message)=> {

            if(message.loggerStatus === Logger.ONLINE) {

                const insertQuery = `INSERT INTO system_status (timestamptz, is_system_up) VALUES ( $1, TRUE)`;
                await conn.connect();
                
                parentPort?.postMessage({ status : Logger.LOGGER_ONLINE});

                intervalHandler = setInterval( ()=> {

                    conn.query(insertQuery, [new Date()]);

                }, 2000);

            }else if(message.loggerStatus === Logger.OFFLINE) {
                
                if(intervalHandler) {
                    
                    await conn.end();
                    clearInterval(intervalHandler);
                    parentPort?.postMessage({ status : Logger.LOGGER_OFFLINE});
                }
            }
        });
    }
}

foomain();