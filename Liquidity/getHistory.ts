// In getHistory.ts
import { ethers, Result, EventLog } from 'ethers';
import { createObjectCsvWriter } from 'csv-writer';
import * as abiModule from './abi.json';
import * as fs from 'fs';
import { parse } from 'csv-parse';

// --- CONFIGURATION ---
const RPC_URL = 'https://bsc-mainnet.infura.io/v3/{infura_id}';  //infura_id is required here
const CONTRACT_ADDRESS = '0x43C3EBaFdF32909aC60E80ee34aE46637E743d65';
const DEPLOYMENT_BLOCK = 26087006;
// --- NEW: Set a specific end block for the query ---
const END_BLOCK = 55130244; 
const OUTPUT_FILE = './history.csv';
const BATCH_SIZE = 10000;
const DELAY_BETWEEN_BATCHES_MS = 500;
const CSV_FILE_PATH = './export-0x43c3ebafdf32909ac60e80ee34ae46637e743d65.csv';

/**
 * Creates a delay for a specified number of milliseconds.
 * @param ms The number of milliseconds to wait.
 */
function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Helper function to clean up event arguments.
 * @param args The event.args object from an ethers log.
 * @returns A clean JSON-compatible object.
 */
function formatEventArgs(args: Result): Record<string, string> {
    const formatted: Record<string, string> = {};
    const argKeys = args.toObject();

    for (const key in argKeys) {
        const value = argKeys[key];
        formatted[key] = typeof value === 'bigint' ? value.toString() : String(value);
    }
    return formatted;
}

async function getBlockNumbersFromCsv(filePath: string): Promise<Set<number>> {
    const blockNumbers = new Set<number>();
    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(parse({ columns: true, skip_empty_lines: true }))
            .on('data', (row) => {
                if (row.Blockno) {
                    blockNumbers.add(parseInt(row.Blockno, 10));
                }
            })
            .on('end', () => {
                console.log(`Extracted ${blockNumbers.size} unique block numbers from ${filePath}`);
                resolve(blockNumbers);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}

// --- MAIN SCRIPT ---
async function downloadAllEvents() {
    console.log("Connecting to the blockchain via RPC...");
    const provider = new ethers.JsonRpcProvider(RPC_URL);

    const contractAbi = (abiModule as any).default || abiModule;
    const contract = new ethers.Contract(CONTRACT_ADDRESS, contractAbi, provider);

    const csvWriter = createObjectCsvWriter({
        path: OUTPUT_FILE,
        header: [
            { id: 'timestamp', title: 'Timestamp' },
            { id: 'blockNumber', title: 'BlockNumber' },
            { id: 'transactionHash', title: 'TransactionHash' },
            { id: 'eventName', title: 'EventName' },
            { id: 'eventData', title: 'EventData' },
        ],
    });

    console.log(`Targeting contract: ${CONTRACT_ADDRESS}`);
    console.log(`Reading block numbers from ${CSV_FILE_PATH}...`);
    const targetBlockNumbers = await getBlockNumbersFromCsv(CSV_FILE_PATH);
    const sortedBlockNumbers = Array.from(targetBlockNumbers).sort((a, b) => a - b);

    console.log(`Fetching events for ${sortedBlockNumbers.length} specific block numbers.`);
    console.log("This may take a while...");

    let allEvents: EventLog[] = [];

    for (const blockNumber of sortedBlockNumbers) {
        console.log(`Fetching events for block ${blockNumber}...`);
        try {
            const eventsInBlock = await contract.queryFilter('*', blockNumber, blockNumber);
            allEvents = allEvents.concat(eventsInBlock as EventLog[]);
            console.log(`Found ${eventsInBlock.length} events in block ${blockNumber}. Total events so far: ${allEvents.length}`);
        } catch (error: any) {
            console.error(`Error fetching events for block ${blockNumber}:`, error.shortMessage || error.message);
        }
        await delay(DELAY_BETWEEN_BATCHES_MS); // Use the existing delay for rate limiting
    }

    console.log(`\nFound a total of ${allEvents.length} events.`);
    console.log("Processing and preparing data for CSV...");

    const records = [];
    const blockTimestampCache: { [blockNumber: number]: number } = {};

    for (const event of allEvents) {
        if (!('eventName' in event) || !event.eventName || !('args' in event) || !event.args) continue;

        if (!blockTimestampCache[event.blockNumber]) {
            try {
                const block = await provider.getBlock(event.blockNumber);
                blockTimestampCache[event.blockNumber] = block?.timestamp ?? 0;
            } catch (e) {
                console.warn(`Could not fetch block ${event.blockNumber}, timestamp will be 0.`);
                blockTimestampCache[event.blockNumber] = 0;
            }
        }
        const timestamp = blockTimestampCache[event.blockNumber];
        const formattedArgs = formatEventArgs(event.args);

        records.push({
            timestamp: new Date(timestamp * 1000).toISOString(),
            blockNumber: event.blockNumber,
            transactionHash: event.transactionHash,
            eventName: event.eventName,
            eventData: JSON.stringify(formattedArgs),
        });
    }

    await csvWriter.writeRecords(records);
    console.log(`\nSUCCESS! All ${records.length} event logs have been written to ${OUTPUT_FILE}`);
}

// --- RUN THE SCRIPT ---
downloadAllEvents().catch(error => {
    console.error("An error occurred during the download process:", error);
    process.exit(1);
});