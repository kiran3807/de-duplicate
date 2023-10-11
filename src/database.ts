import * as dotenv from "dotenv";
import { Client } from "pg";

dotenv.config();

/*
    We encapsulate all functionalities pertaining to the database querying and processing
    within this class and we retrieve connection details from the environment variables.
*/
export class Database {

    private conn: Client | null = null;
    private jobsTable = "jobs_raw";
    private isValidColumn = "is_valid";
    private systemStatusTable = "system_status";
    private systemStatusTimeStamp = "timestamptz";
    private systemStatusIsUp = "is_system_up";

    constructor() {
        dotenv.config();
    }

    async connect() {
        this.conn = new Client({
            user: process.env.DB_USER,
            host: process.env.DB_HOST,
            database: process.env.DB_NAME,
            password: process.env.DB_PASSWORD,
            port: 5432
        });

        this.conn.connect();
    }


    async getSortedByTitleRecords() {

        try {
            const getSortedByTitlesQuery = `SELECT * FROM ${this.jobsTable} ORDER BY LENGTH(title) DESC;`;
            return (await this.conn!.query(getSortedByTitlesQuery)).rows;

        } catch(dbError: any) {
            console.log(dbError);
        }
    }

    async addIsValidColumn() {
        
        try {

            const alterTableQuery = `ALTER TABLE ${this.jobsTable} ADD COLUMN ${this.isValidColumn} BOOLEAN;`;
            await this.conn!.query(alterTableQuery);
            console.log("is_valid column added to raw-jobs");

        } catch(dbError: any) {
            console.log(dbError);
        }
    }

    async setIsValidRecord(validIdList: number[]) {

        try{

            let idList = validIdList.map(id=>id.toString()).reduce((idList: string, id: string)=> {
                
                idList = idList + "," + id;
                return idList;
            });
    
            let updateIsValidQuery = `UPDATE ${this.jobsTable} SET is_valid = TRUE WHERE id IN (${idList});`;
            await this.conn!.query(updateIsValidQuery);

        } catch(dbError: any) {
            console.log(dbError);
        }

        return
    }

    async setStatus() {

        try {

            const insertQuery = `INSERT INTO ${this.systemStatusTable} (${this.systemStatusTimeStamp}, ${this.systemStatusIsUp}) VALUES ( $1, TRUE)`;
            await this.conn!.query(insertQuery, [new Date()]);

        }catch(dbError: any) {
            console.log(dbError);
        }
    }

    async closeConnection() {
        this.conn!.end();
    }
}