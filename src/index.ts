import { Client } from "pg";
import * as dotenv from "dotenv";

async function main() {

    dotenv.config();

    const client = new Client({
        user: process.env.DB_USER,
        host: process.env.DB_HOST,
        database: process.env.DB_NAME,
        password: process.env.DB_PASSWORD,
        port: 5432
    });

    try {

        client.connect();

        let result = await client.query("SELECT * FROM jobs_raw LIMIT 10");
        for(let row of result.rows) {
            console.log(row);
        }

    } catch(dbError: any) {
        console.log(dbError);
    }
}

main();