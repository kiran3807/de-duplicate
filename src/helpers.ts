import assert from "assert";

import { DeDuplicatedRow, ValidRow } from "./types";

/*
    Duplicates are determined by iterating through both the strings
    and testing whether the order to the letters match. Whenever the orders
    stop matching we check if the length covered in the shorter stringso far is 
    80 percent of the larger string or more, if so then we declare the shorter string a duplicate
*/
export function checkDuplicate(base: string, duplicate: string): boolean {

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

/*

    Since the records are obtained sorted in descending fashion, we start with the largest string
    present at first index and find out all other titles that are its duplicates with the help
    of function above, simultaneously we eliminate the duplicates from the records.

    Subsequently we start at the beginning again, with the string left at the top is again by default
    the largest due to the ordering of the records and nature of duplicate deletions

    We continue the process untill we have emptied the collection of all records.
*/
export function deDuplicate(rows: any) {

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

/*
    A utility function designed to neatly present the ids at which UPDATE operation needs to
    be conducted in order to mark rows associated with those ids as valid
*/
export function filterValidSources(deDupRows: DeDuplicatedRow[]) {

    let validRows: ValidRow[] = [];

    validRows = deDupRows.map(row=> {
        
        let validRow = {} as ValidRow;

        let validSourceKey = Object.keys(row.sources).reduce((validKey: string, key: string)=> {

            if(row.sources[key].length > row.sources[validKey].length) {
                return key;
            }
            return validKey;
        });

        validRow["title"] = row.title;
        validRow["ids"] = row.sources[validSourceKey];

        return validRow;
    });

    return validRows;
}