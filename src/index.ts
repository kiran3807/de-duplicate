import { Client } from "pg";
import * as dotenv from "dotenv";
import assert from "assert";

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

    let eightyPercent = base.length * (4/5);
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

    let dedupRows: any[] = [];

    while(rows.length > 0) {
        
        let baseRow = rows.shift();
        dedupRows.push({ 
            title : baseRow.title,  
            sources : {
                
                [baseRow.source_type] : { 
                    count : 1, 
                    ids : [ baseRow.id ] 
                }
            } });

        rows = rows.filter((row: any)=> {

            let lastDedupRow = dedupRows[dedupRows.length - 1];

            if(checkDuplicate(lastDedupRow.title, row.title)) {

                if(lastDedupRow.sources[row.source_type]) {

                    lastDedupRow.sources[row.source_type].count++;
                    lastDedupRow.sources[row.source_type].ids.push(row.id);

                } else {

                    lastDedupRow.sources[row.source_type] = { count : 1, ids : [ row.id ]};
                }

                return false;
            }
            return true;
        });
    }

    return dedupRows;

}

async function main() {

    let connection = await getDBConnection();

    try {

        connection.connect();

        const getSortedTitlesQuery = "SELECT * FROM jobs_raw ORDER BY LENGTH(title) DESC;";

        let result = await connection.query(getSortedTitlesQuery);
        await connection.end()
        
        let deDuplicatedRows = deDuplicate(result.rows);

        /*for(let d of deDuplicatedRows) {
            console.log("---------");
            console.log(d.title);
            Object.keys(d.sources).forEach(key=>console.log(key, d.sources[key].count, d.sources[key].ids));
            console.log("---------");
        }*/

    } catch(dbError: any) {
        console.log(dbError);
    }
}

main();