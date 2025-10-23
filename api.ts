import express from 'express';
import type { Express, Request, Response } from 'express';
import * as fs from 'fs';
import { parse } from 'csv-parse/sync';

const app: Express = express();
const port = 3000;

// =================================================================
// INTERFACES & CONFIGURATION
// =================================================================

interface CandleRecord {
  'Unix Timestamp': string;
  'Open': string;
  'Close': string;
  'High': string;
  'Low': string;
  'tVol': string;
}

// Interfaces for response types
interface CandleDataNumber {
  unixTimestamp: number; readableTimestamp: string; open: number; high: number;
  low: number; close: number; volume: number;
}
interface CandleDataString {
  unixTimestamp: number; readableTimestamp: string; open: string; high: string;
  low: string; close: string; volume: string;
}

// Interface for internal BigInt calculations
interface CandleDataBigInt {
    open: bigint; high: bigint; low: bigint; close: bigint; volume: bigint;
}

const timeframeMap: { [key: string]: number } = {
  '1m': 60, '5m': 300, '15m': 900, '30m': 1800, '45m': 2700,
  '1h': 3600, '2h': 7200, '3h': 10800, '4h': 14400, '5h': 18000,
  '6h': 21600, '12h': 43200, '15h': 54000, '18h': 64800, '1d': 86400,
};

// =================================================================
// CORE LOGIC & HELPER FUNCTIONS
// =================================================================

function getCandleRecords(): CandleRecord[] {
  try {
    const csvData = fs.readFileSync('candle_data.csv', 'utf8');
    const records = parse(csvData, { columns: true, skip_empty_lines: true }) as CandleRecord[];
    records.sort((a, b) => parseInt(a['Unix Timestamp']) - parseInt(b['Unix Timestamp']));
    return records;
  } catch (error) {
    console.error("Fatal error reading or parsing candle_data.csv:", error);
    throw new Error('Could not process CSV file.');
  }
}

/**
 * A centralized function to aggregate raw transaction data into OHLCV candles.
 * It can return values as floating-point numbers or as full-precision strings.
 */
function aggregateCandleData(records: CandleRecord[], timeframeInSeconds: number, outputAsString: boolean): (CandleDataNumber | CandleDataString)[] {
    if (outputAsString) {
        // --- Logic for Full Precision Strings using BigInt ---
        const aggregatedCandles: { [key: number]: CandleDataBigInt } = {};
        for (const record of records) {
            try {
                const timestamp = parseInt(record['Unix Timestamp'], 10);
                const open = BigInt(record.Open); const high = BigInt(record.High);
                const low = BigInt(record.Low); const close = BigInt(record.Close);
                const volume = BigInt(record.tVol);
                const bucketTimestamp = Math.floor(timestamp / timeframeInSeconds) * timeframeInSeconds;

                if (!aggregatedCandles[bucketTimestamp]) {
                    aggregatedCandles[bucketTimestamp] = { open, high, low, close, volume };
                } else {
                    const candle = aggregatedCandles[bucketTimestamp];
                    candle.high = high > candle.high ? high : candle.high;
                    candle.low = low < candle.low ? low : candle.low;
                    candle.close = close;
                    candle.volume += volume;
                }
            } catch { continue; }
        }
        return Object.entries(aggregatedCandles).map(([timestampStr, ohlcv]) => ({
            unixTimestamp: parseInt(timestampStr, 10),
            readableTimestamp: new Date(parseInt(timestampStr, 10) * 1000).toISOString(),
            open: ohlcv.open.toString(),
            high: ohlcv.high.toString(),
            low: ohlcv.low.toString(),
            close: ohlcv.close.toString(),
            volume: ohlcv.volume.toString(),
        })).sort((a, b) => a.unixTimestamp - b.unixTimestamp);
    } else {
        // --- Original Logic for Floating Point Numbers ---
        const aggregatedCandles: { [key: number]: CandleDataNumber } = {};
        for (const record of records) {
            const timestamp = parseInt(record['Unix Timestamp'], 10);
            const open = parseFloat(record.Open); const high = parseFloat(record.High);
            const low = parseFloat(record.Low); const close = parseFloat(record.Close);
            const volume = parseFloat(record.tVol);
            if ([timestamp, open, high, low, close, volume].some(isNaN)) continue;
            const bucketTimestamp = Math.floor(timestamp / timeframeInSeconds) * timeframeInSeconds;
            if (!aggregatedCandles[bucketTimestamp]) {
                aggregatedCandles[bucketTimestamp] = {
                    unixTimestamp: bucketTimestamp, readableTimestamp: new Date(bucketTimestamp * 1000).toISOString(),
                    open, high, low, close, volume
                };
            } else {
                const candle = aggregatedCandles[bucketTimestamp];
                candle.high = Math.max(candle.high, high);
                candle.low = Math.min(candle.low, low);
                candle.close = close;
                candle.volume += volume;
            }
        }
        return Object.values(aggregatedCandles).sort((a, b) => a.unixTimestamp - b.unixTimestamp);
    }
}


// =================================================================
// DYNAMIC API ENDPOINTS
// =================================================================

app.get('/api/candles/:timeframe', (req: Request, res: Response) => {
    const { timeframe } = req.params;
    const { format } = req.query; // Check for ?format=string
    const timeframeInSeconds = timeframeMap[timeframe];

    if (!timeframeInSeconds) {
        return res.status(400).json({ 
            error: "Invalid timeframe specified.",
            availableTimeframes: Object.keys(timeframeMap)
        });
    }

    try {
        const records = getCandleRecords();
        const candleData = aggregateCandleData(records, timeframeInSeconds, format === 'string');
        res.json(candleData);
    } catch (error) {
        console.error(`Error in /api/candles/${timeframe}:`, error);
        res.status(500).send('Error fetching candle data');
    }
});

app.get('/api/ohlc/:timeframe', (req: Request, res: Response) => {
    const { timeframe } = req.params;
    const { format } = req.query;
    const timeframeInSeconds = timeframeMap[timeframe];

    if (!timeframeInSeconds) {
        return res.status(400).json({ error: "Invalid timeframe.", availableTimeframes: Object.keys(timeframeMap) });
    }

    try {
        const records = getCandleRecords();
        const fullCandles = aggregateCandleData(records, timeframeInSeconds, format === 'string');
        const ohlcHistory = fullCandles.map(c => ({
            unixTimestamp: c.unixTimestamp, readableTimestamp: c.readableTimestamp,
            open: c.open, high: c.high, low: c.low, close: c.close
        }));
        res.json(ohlcHistory);
    } catch (error) {
        console.error(`Error in /api/ohlc/${timeframe}:`, error);
        res.status(500).send('Error fetching OHLC data');
    }
});

app.get('/api/volume/:timeframe', (req: Request, res: Response) => {
    const { timeframe } = req.params;
    const { format } = req.query;
    const timeframeInSeconds = timeframeMap[timeframe];

    if (!timeframeInSeconds) {
        return res.status(400).json({ error: "Invalid timeframe.", availableTimeframes: Object.keys(timeframeMap) });
    }

    try {
        const records = getCandleRecords();
        const fullCandles = aggregateCandleData(records, timeframeInSeconds, format === 'string');
        const volumeHistory = fullCandles.map(c => ({
            unixTimestamp: c.unixTimestamp,
            readableTimestamp: c.readableTimestamp,
            volume: c.volume,
        }));
        res.json(volumeHistory);
    } catch (error) {
        console.error(`Error in /api/volume/${timeframe}:`, error);
        res.status(500).send('Error fetching volume data');
    }
});


app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
  console.log("\n--- Available Dynamic Endpoints ---");
  console.log(" -> /api/candles/:timeframe  (OHLC + Volume)");
  console.log(" -> /api/ohlc/:timeframe     (OHLC only)");
  console.log(" -> /api/volume/:timeframe   (Volume only)");
  console.log("\nAppend '?format=string' to any endpoint for full precision numbers.");
  console.log("\nAvailable timeframes:", Object.keys(timeframeMap).join(', '));
});