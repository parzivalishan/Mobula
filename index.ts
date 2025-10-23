import { ethers } from "ethers";
import * as fs from 'fs';
import { stringify } from 'csv-stringify';
import * as readlineSync from 'readline-sync';
import { format } from 'date-fns';

const rpcUrl = readlineSync.question('Enter RPC URL: ');
const contractAddress = readlineSync.question('Enter Contract Address: ');

if (!rpcUrl || !contractAddress) {
  console.error("RPC URL and Contract Address are required.");
  process.exit(1);
}

const provider = new ethers.JsonRpcProvider(rpcUrl);

const abi = [
  "function totalTx() view returns (uint256)",
  "function txTimeStamp(uint256) view returns (uint256)",
  "function candleStickData(uint256) view returns (uint256 time, uint256 open, uint256 close, uint256 high, uint256 low)",
];

// Minimal ABI for the tVol public mapping
const tVolABI = [
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "",
        "type": "uint256"
      }
    ],
    "name": "tVol",
    "outputs": [
      {
        "internalType": "uint256",
        "name": "",
        "type": "uint256"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  }
];

const contract = new ethers.Contract(contractAddress, abi, provider);
const tVolContract = new ethers.Contract(contractAddress, tVolABI, provider);

async function fetchData() {
  console.log("=== SGR20 Historical Market Indicators ===\n");

  try {
    // 1. Fetch totalTx
    const totalTxBigInt = await contract.totalTx();
    const totalTx = Number(totalTxBigInt);
    console.log(`Total recorded transactions: ${totalTx}\n`);

    const allCandleData: any[] = [];
    const tVolData: { Timestamp: number; tVol: string; utcDate?: string; dailyTotalVolume?: string; cumulativeDailyVolume?: string; cumulativeTotalVolume?: string }[] = [];

    // Add header row to CSV data
    allCandleData.push([
      "Total Transactions",
      "Transaction Index",
      "Unix Timestamp",
      "Readable Timestamp",
      "Open",
      "Close",
      "High",
      "Low",
      "tVol",
      "UTC Date",
      "Daily Total Volume",
      "Cumulative Daily Volume",
      "Cumulative Total Volume (Till Date)"
    ]);

    let cumulativeTotalVolume = BigInt(0);
    let currentDay: string | undefined;
    let currentDayCumulativeVolume = BigInt(0);

    // 2. Iterate from 1 to totalTx
    for (let i = 1; i <= totalTx; i++) {
      // 3. Get timestamp for each transaction
      const timestampBigInt = await contract.txTimeStamp(i);
      const timestamp = Number(timestampBigInt);
      console.log(`Fetching candle data for transaction index ${i} (unix timestamp: ${timestamp})...`);

      // Convert Unix timestamp to human-readable format
      const date = new Date(timestamp * 1000); // Convert to milliseconds
      const readableTimestamp = date.toUTCString();
      const utcDate = date.toISOString().substring(0, 10);

      // 4. Get candleStickData from the timestamp
      const candle = await contract.candleStickData(timestamp);

      // 5. Output raw candleStickData and store in array
      const candleData = {
        time: Number(candle.time),
        open: candle.open.toString(),
        close: candle.close.toString(),
        high: candle.high.toString(),
        low: candle.low.toString(),
      };
      console.log(`CandleData for timestamp ${timestamp}:`, candleData);

      // Fetch tVol
      let tVol = 'Error';
      try {
        const volume = await tVolContract.tVol(timestamp);
        tVol = volume.toString();
        console.log(`Fetched tVol for timestamp ${timestamp}: ${tVol} (UTC Date: ${utcDate})`);

        // Update cumulative volumes
        if (currentDay === undefined || currentDay !== utcDate) {
          currentDay = utcDate;
          currentDayCumulativeVolume = BigInt(0);
        }
        currentDayCumulativeVolume += BigInt(tVol);
        cumulativeTotalVolume += BigInt(tVol);

      } catch (error) {
        console.error(`Error fetching tVol for timestamp ${timestamp}:`, error);
      }

      tVolData.push({ Timestamp: timestamp, tVol: tVol, utcDate: utcDate, cumulativeDailyVolume: currentDayCumulativeVolume.toString(), cumulativeTotalVolume: cumulativeTotalVolume.toString() });

      allCandleData.push([
        totalTx,
        i,
        timestamp,
        readableTimestamp,
        candleData.open,
        candleData.close,
        candleData.high,
        candleData.low,
        tVol,
        utcDate,
        '' ,// Placeholder for daily total volume
        currentDayCumulativeVolume.toString(),
        cumulativeTotalVolume.toString()
      ]);
      console.log("\n"); // Add a newline for better readability between entries
    }

    const dailyVolumeMap = new Map<string, bigint>();

    for (const data of tVolData) {
      if (data.utcDate && data.tVol !== 'Error') {
        const currentDailyVolume = dailyVolumeMap.get(data.utcDate) || BigInt(0);
        dailyVolumeMap.set(data.utcDate, currentDailyVolume + BigInt(data.tVol));
      }
    }

    // Update allCandleData with daily total volume
    for (let i = 1; i < allCandleData.length; i++) { // Start from 1 to skip header
      const utcDate = allCandleData[i][9]; // UTC Date is at index 9
      if (utcDate) {
        allCandleData[i][10] = dailyVolumeMap.get(utcDate)?.toString() || '0'; // Daily Total Volume is at index 10
      }
    }

    // Write data to CSV file
    stringify(allCandleData, (err, output) => {
      if (err) {
        console.error("Error writing CSV:", err);
      } else {
        fs.writeFileSync('candle_data.csv', output);
        console.log("Data successfully written to candle_data.csv");
      }
    });

  } catch (err) {
    console.error("Error:", err);
  }
}

fetchData();
