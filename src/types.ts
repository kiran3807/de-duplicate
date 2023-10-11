export type DeDuplicatedRow = {
    title : string,
    sources : {
        [key: string] : number[]
    }
}

export type ValidRow = {
    title : string,
    ids : number[]
}

export enum Logger {
    ONLINE,
    OFFLINE,
    LOGGER_ONLINE,
    LOGGER_OFFLINE
}