import * as hl from "@nktkas/hyperliquid";
import WebSocket from "ws";
import * as fs from "fs/promises";
import * as path from "path";
import fetch from "node-fetch";
import { createWriteStream, WriteStream } from "fs";
import { stringify } from "csv-stringify";
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import Decimal from 'decimal.js';
import { ethers } from 'ethers';
import { once } from "events";
import winston from 'winston';
import {
  Token,
  CurrencyAmount,
  TradeType,
  Percent,
} from "@uniswap/sdk-core";
import { Pool, Route, Trade, SwapRouter, SwapOptions } from "@uniswap/v3-sdk";
import JSBI from "jsbi";

// ==================== Logger Setup ====================
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'logs/error.log', level: 'error' }),
    new winston.transports.File({ filename: 'logs/combined.log' }),
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    })
  ]
});

// ==================== Decimal.js Configuration ====================
Decimal.set({ precision: 60, rounding: Decimal.ROUND_HALF_UP });

// ==================== Environment Variables ====================
const WALLET_CONFIG = {
  CORE: {
    address: "1111111111111111111111111111111111111111111111",
    privateKey: process.env.HL_PRIVATE_KEY || "1111111111111111111111111111111111111111111111111111"
  },
  EVM: {
    address: "1111111111111111111111111111111111111111111111",
    privateKey: process.env.EVM_PRIVATE_KEY || "1111111111111111111111111111111111111111111111111111"
  }
} as const;

// 검증
if (!WALLET_CONFIG.CORE.privateKey || !WALLET_CONFIG.EVM.privateKey) {
  logger.error("❌ Private keys not set!");
  process.exit(1);
}

// ==================== Configuration ====================
const CONFIG = {
  AUTO_EXECUTE: process.env.AUTO_EXECUTE === 'true' || false,
  MIN_PROFIT_PCT: parseFloat(process.env.MIN_PROFIT_PCT || '0.5'),
  MAX_POSITION_USD: parseFloat(process.env.MAX_POSITION_USD || '20'),
  MAX_GAS_PRICE_GWEI: parseFloat(process.env.MAX_GWEI || '10'), // Increased from 5 to 10
  GAS_PRICE_WARNING_GWEI: 5, // New threshold for warnings
  MIN_TRADE_USD: 10,
  BRIDGE_DELAY_MS: 3000,
  HEALTH_CHECK_INTERVAL_MS: 60000,
  DEX_UPDATE_INTERVAL_MS: 100, // 10초 -> 1초로 변경
  SLIPPAGE_TOLERANCE: 1.0, // 1%
} as const;

// ==================== Contract Addresses ====================
const HYPEREVM_RPC = process.env.HYPEREVM_RPC || "https://rpc.hyperliquid.xyz/evm";
const CHAIN_ID = 999;

// Upheaval (Uniswap V3 Fork)
const UPHEAVAL_ROUTER = "0xf49A33dF1Bb5DA03B85396a768666523c1b7Eb9e"; // ✅ CORRECT ADDRESS
const UPHEAVAL_SUBGRAPH = "https://api.upheaval.fi/subgraphs/name/upheaval/exchange-v3";

// Token Addresses
const USDT0_ADDRESS = "0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb";
const WHYPE_ADDRESS = "0x5555555555555555555555555555555555555555";
const PUP_ADDRESS = "0x876e7f2f30935118a654fc0e1f807afc49efe500";
const UPHL_ADDRESS = "0x9c3bcc457862dd6d8907ba11f8c7fe7e43a8c08b";

// System Contract for Bridging
const HYPE_SYSTEM_ADDRESS = "0x2222222222222222222222222222222222222222";

const TOKEN_DECIMALS: Record<string, number> = {
  USDT0: 6,
  PUP: 18,
  HYPE: 18,
  UPHL: 18
};

const TOKEN_ADDRESSES: Record<string, string> = {
  PUP: PUP_ADDRESS,
  HYPE: WHYPE_ADDRESS,
  UPHL: UPHL_ADDRESS,
};

// Upheaval Pool Addresses (from your working code)
const UPHEAVAL_POOLS: Record<string, { address: string; token0: string; token1: string; fee: number }> = {
  "PUP": {
    address: "0xe9c02ca07931f9670fa87217372b3c9aa5a8a934",
    token0: PUP_ADDRESS,
    token1: WHYPE_ADDRESS,
    fee: 3000 // 0.3%
  },
  "HYPE": {
    address: "0x6df19a40aaf19d01c2616a1765d4d2a4842bffaf",
    token0: WHYPE_ADDRESS,
    token1: USDT0_ADDRESS,
    fee: 3000
  },
  "UPHL": {
    address: "0x49c6e25f156eea9fe340e25087c76a6aad7c610a",
    token0: UPHL_ADDRESS,
    token1: WHYPE_ADDRESS,
    fee: 3000
  }
};

// Fee Constants
const HYPERLIQUID_FEE_BPS = 7; // 0.07%
const UPHEAVAL_FEE_BPS = 30; // 0.3%
const BRIDGE_FEE_PCT = 0.1;
const ESTIMATED_GAS_COST_USD = 0.5;
const FIRST_LEVEL_FILL_RATIO = 0.85;

// ==================== Interfaces ====================
interface OrderbookLevel {
  level: number;
  price: string;
  size: string;
  cumulative: string;
  timestamp: string;
}

interface CoinOrderbook {
  symbol: string;
  displaySymbol: string;
  lastUpdate: string;
  midPrice: string;
  spread: string;
  spreadPercentage: string;
  bestBid: number;
  bestAsk: number;
  bids: OrderbookLevel[];
  asks: OrderbookLevel[];
}

interface UpheavalPoolData {
  symbol: string;
  poolAddress: string;
  price: number;
  priceInUSDT: number;
  liquidity: string;
  sqrtPriceX96: string;
  tick: number;
  feeTier: number;
  token0: { address: string; decimals: number; symbol: string };
  token1: { address: string; decimals: number; symbol: string };
}

interface ArbitrageRoute {
  token: string;
  direction: string;
  buyProtocol: string;
  sellProtocol: string;
  buyPrice: number;
  sellPrice: number;
  priceGap: number;
  optimalAmount: number;
  netProfit: number;
  estimatedProfitUSD: number;
  buyPriceImpact: number;
  sellPriceImpact: number;
  buyExecutionPrice: number;
  sellExecutionPrice: number;
  status: "PROFITABLE" | "NEGATIVE" | "NO_DATA";
}

// ==================== TTL Cache ====================
class TTLMap<K, V> {
  private data: Map<K, { v: V; t: number }>;
  private ttlMs: number;

  constructor(ttlMs = 60_000) {
    this.data = new Map();
    this.ttlMs = ttlMs;
  }

  set(k: K, v: V): this {
    this.data.set(k, { v, t: Date.now() });
    return this;
  }

  get(k: K): V | undefined {
    const ent = this.data.get(k);
    if (ent && Date.now() - ent.t < this.ttlMs) return ent.v;
    this.data.delete(k);
    return undefined;
  }

  getAllValid(): Map<K, V> {
    const result = new Map<K, V>();
    const now = Date.now();
    for (const [k, { v, t }] of this.data) {
      if (now - t < this.ttlMs) {
        result.set(k, v);
      } else {
        this.data.delete(k);
      }
    }
    return result;
  }

  sweep(): void {
    const now = Date.now();
    for (const [k, { t }] of this.data) {
      if (now - t >= this.ttlMs) this.data.delete(k);
    }
  }

  get size(): number {
    this.sweep();
    return this.data.size;
  }
}

// ==================== Circuit Breaker ====================
class CircuitBreaker {
  private failures = 0;
  private lastFailTime = 0;
  private readonly threshold = 3;
  private readonly resetTime = 60000;
  
  async execute<T>(fn: () => Promise<T>, operation: string): Promise<T> {
    if (this.failures >= this.threshold) {
      if (Date.now() - this.lastFailTime < this.resetTime) {
        throw new Error(`Circuit breaker open for ${operation}`);
      }
      logger.info(`[Circuit] Resetting after timeout`);
      this.failures = 0;
    }
    
    try {
      const result = await fn();
      this.failures = 0;
      return result;
    } catch (error) {
      this.failures++;
      this.lastFailTime = Date.now();
      logger.error(`[Circuit] Failure ${this.failures}/${this.threshold} for ${operation}`);
      throw error;
    }
  }
}

// ==================== Token Mutex ====================
class TokenMutex {
  private locks = new Map<string, Promise<void>>();
  
  async acquire(token: string): Promise<() => void> {
    while (this.locks.has(token)) {
      await this.locks.get(token);
    }
    
    let release: () => void;
    const promise = new Promise<void>(resolve => {
      release = resolve;
    });
    
    this.locks.set(token, promise);
    
    return () => {
      this.locks.delete(token);
      release!();
    };
  }
}

// ==================== Global Variables ====================
const TARGET_SYMBOLS = ["PUP", "HYPE", "UPHL"];

let isRunning = true;
const tokenMutex = new TokenMutex();
const circuitBreaker = new CircuitBreaker();
const spotSymbolMapping = new Map<string, string>();
const reverseLookup = new Map<string, string>();

// Hyperliquid Clients
const httpTransport = new hl.HttpTransport();
const publicClient = new hl.PublicClient({ transport: httpTransport });
const hlWallet = new ethers.Wallet(WALLET_CONFIG.CORE.privateKey);
const exchangeClient = new hl.WalletClient({ 
  wallet: hlWallet, 
  transport: httpTransport,
  address: WALLET_CONFIG.CORE.address // 지갑 주소 추가
});

// EVM Provider
let evmProvider: ethers.JsonRpcProvider;
let evmWallet: ethers.Wallet;

const incrementalOrderbooks = new TTLMap<string, CoinOrderbook>(30_000);
const targetMidsCache = new Map<string, string>();
const upheavalPrices = new Map<string, UpheavalPoolData>();

let outputDir: string;
let csvWriteStream: WriteStream | null = null;
let csvStringifier: any = null;
let dexUpdateInterval: NodeJS.Timeout;
let ttlSweepInterval: NodeJS.Timeout;
let healthCheckInterval: NodeJS.Timeout;

// ==================== Utility Functions ====================
function calculatePriceFromSqrtPrice(
  sqrtPriceX96: string,
  decimals0: number,
  decimals1: number
): number {
  try {
    const Q96 = new Decimal(2).pow(96);
    const sqrtP = new Decimal(sqrtPriceX96);
    const price = sqrtP.div(Q96).pow(2);
    const adj = new Decimal(10).pow(decimals0 - decimals1);
    return price.mul(adj).toNumber();
  } catch (e) {
    logger.error("[Util] sqrtPrice calculation error:", e);
    return 0;
  }
}

function getSystemAddress(tokenIndex: number): string {
  return "0x20" + tokenIndex.toString(16).padStart(38, "0");
}

async function getSystemAddressForToken(tokenAddress: string): Promise<string> {
  try {
    if (tokenAddress.toLowerCase() === WHYPE_ADDRESS.toLowerCase()) {
      return HYPE_SYSTEM_ADDRESS;
    }
    
    const spotMeta = await publicClient.spotMeta();
    const tokenInfo = spotMeta.tokens.find((t: any) => 
      t.evmContract?.toLowerCase() === tokenAddress.toLowerCase()
    );
    
    if (!tokenInfo) {
      throw new Error(`Token ${tokenAddress} not linked to HyperCore`);
    }
    
    return getSystemAddress(tokenInfo.index);
  } catch (error) {
    logger.error(`[System] Failed to get system address:`, error);
    throw error;
  }
}

// ==================== RPC Initialization ====================
async function initializeProvider(): Promise<void> {
  try {
    evmProvider = new ethers.JsonRpcProvider(HYPEREVM_RPC, CHAIN_ID);
    await evmProvider.getBlockNumber();
    logger.info("[RPC] Connected to HyperEVM");
    
    evmWallet = new ethers.Wallet(WALLET_CONFIG.EVM.privateKey, evmProvider);
    if (evmWallet.address.toLowerCase() !== WALLET_CONFIG.EVM.address.toLowerCase()) {
      throw new Error("EVM wallet address mismatch");
    }
    
    logger.info(`[Wallet] Address: ${evmWallet.address} verified ✓`);
  } catch (error) {
    throw new Error("Wallet initialization failed: " + error);
  }
}

// ==================== CSV Streaming ====================
async function initializeCSVStream(): Promise<void> {
  if (!csvWriteStream) {
    const timestamp = new Date().toISOString().replace(/:/g, "-").split(".")[0];
    const csvPath = path.join(outputDir, `arbitrage_${timestamp}.csv`);
    
    csvWriteStream = createWriteStream(csvPath);
    csvStringifier = stringify({
      header: true,
      columns: {
        timestamp: 'Timestamp',
        token: 'Token',
        direction: 'Direction',
        buyProtocol: 'Buy_Protocol',
        sellProtocol: 'Sell_Protocol',
        buyPrice: 'Buy_Price',
        sellPrice: 'Sell_Price',
        priceGap: 'Price_Gap_Pct',
        netProfit: 'Net_Profit_Pct',
        estimatedProfit: 'Est_Profit_USD',
        optimalAmount: 'Optimal_Amount_USD',
        status: 'Status',
        executed: 'Executed',
        error: 'Error'
      }
    });
    
    csvStringifier.pipe(csvWriteStream);
  }
}

async function appendToCSV(routes: ArbitrageRoute[], executed = false, error = ""): Promise<void> {
  if (!csvStringifier || !isRunning) return;

  try {
    const timestamp = new Date().toISOString();
    for (const route of routes) {
      const ok = csvStringifier.write({
        timestamp,
        token: route.token,
        direction: route.direction,
        buyProtocol: route.buyProtocol,
        sellProtocol: route.sellProtocol,
        buyPrice: route.buyPrice.toFixed(8),
        sellPrice: route.sellPrice.toFixed(8),
        priceGap: route.priceGap.toFixed(4),
        netProfit: route.netProfit > -999 ? route.netProfit.toFixed(4) : "N/A",
        estimatedProfit: route.estimatedProfitUSD.toFixed(2),
        optimalAmount: route.optimalAmount.toFixed(2),
        status: route.status,
        executed: executed ? 'YES' : 'NO',
        error: error
      });

      if (!ok) await once(csvStringifier, "drain");
    }
  } catch (error) {
    logger.error("[CSV] Error:", error);
  }
}

// ==================== Health Check ====================
async function performHealthCheck(): Promise<boolean> {
  try {
    const blockNumber = await evmProvider.getBlockNumber();
    const evmBalance = await evmProvider.getBalance(evmWallet.address);
    
    if (evmBalance < ethers.parseEther("0.05")) {
      logger.error(`[Health] Low gas balance: ${ethers.formatEther(evmBalance)} HYPE`);
      return false;
    }
    
    const feeData = await evmProvider.getFeeData();
    const gasPriceGwei = parseFloat(ethers.formatUnits(feeData.gasPrice || 0n, "gwei"));
    
    // Log warning but don't stop if gas price is above warning threshold
    if (gasPriceGwei > CONFIG.GAS_PRICE_WARNING_GWEI) {
      logger.warn(`[Health] High gas price: ${gasPriceGwei.toFixed(2)} gwei`);
    }
    
    // Only fail health check if gas price is above maximum threshold
    if (gasPriceGwei > CONFIG.MAX_GAS_PRICE_GWEI) {
      logger.error(`[Health] Gas price too high: ${gasPriceGwei.toFixed(2)} gwei`);
      return false;
    }
    
    logger.info(`[Health] ✓ Block: ${blockNumber}, Gas: ${gasPriceGwei.toFixed(2)} gwei, Balance: ${ethers.formatEther(evmBalance).slice(0, 6)} HYPE`);
    return true;
  } catch (error: any) {
    logger.error("[Health] Check failed:", error);
    return false;
  }
}

async function validateBalances(): Promise<boolean> {
  try {
    const l1State = await publicClient.clearinghouseState({ user: hlWallet.address });
    const l1UsdtBalance = parseFloat(l1State.marginSummary.accountValue || "0");
    
    const erc20 = new ethers.Contract(
      USDT0_ADDRESS,
      ['function balanceOf(address) view returns (uint256)'],
      evmProvider
    );
    const evmUsdtBalance = await erc20.balanceOf(evmWallet.address);
    const evmUsdtUsd = parseFloat(ethers.formatUnits(evmUsdtBalance, 6));
    
    logger.info(`[Balance] L1: ${l1UsdtBalance.toFixed(2)} USD | EVM: ${evmUsdtUsd.toFixed(2)} USDT`);
    
    const totalBalance = l1UsdtBalance + evmUsdtUsd;
    if (totalBalance < CONFIG.MAX_POSITION_USD * 2) {
      logger.warn(`[Balance] Low balance: ${totalBalance.toFixed(2)} USD (need ${CONFIG.MAX_POSITION_USD * 2})`);
      return false;
    }
    
    return true;
  } catch (error) {
    logger.error("[Balance] Check failed:", error);
    return false;
  }
}

// ==================== Upheaval Price Fetching ====================
async function fetchUpheavalPoolData(poolAddress: string, symbol: string): Promise<UpheavalPoolData | null> {
  const query = {
    operationName: "GetPoolInfo",
    variables: { poolId: poolAddress.toLowerCase() },
    query: `
      query GetPoolInfo($poolId: String!) {
        pool(id: $poolId) {
          id
          sqrtPrice
          liquidity
          feeTier
          tick
          token0 { 
            id 
            symbol 
            decimals 
          }
          token1 { 
            id 
            symbol 
            decimals 
          }
        }
      }
    `,
  };

  try {
    const response = await fetch(UPHEAVAL_SUBGRAPH, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(query),
    });
    
    if (!response.ok) {
      logger.error(`[Upheaval] HTTP ${response.status} for ${symbol}`);
      return null;
    }
    
    const data: any = await response.json();
    if (data.errors || !data.data?.pool) {
      logger.error(`[Upheaval] No pool data for ${symbol}`);
      return null;
    }

    const pool = data.data.pool;
    const token0 = pool.token0;
    const token1 = pool.token1;
    
    const price = calculatePriceFromSqrtPrice(
      pool.sqrtPrice,
      parseInt(token0.decimals),
      parseInt(token1.decimals)
    );

    return {
      symbol,
      poolAddress,
      price,
      priceInUSDT: 0, // Will be calculated later
      liquidity: pool.liquidity,
      sqrtPriceX96: pool.sqrtPrice,
      tick: parseInt(pool.tick),
      feeTier: parseInt(pool.feeTier),
      token0: {
        address: token0.id,
        decimals: parseInt(token0.decimals),
        symbol: token0.symbol,
      },
      token1: {
        address: token1.id,
        decimals: parseInt(token1.decimals),
        symbol: token1.symbol,
      },
    };
  } catch (error) {
    logger.error(`[Upheaval] Fetch error for ${symbol}:`, error);
    return null;
  }
}

async function updateUpheavalPrices(): Promise<void> {
  try {
    // First get HYPE/USDT0 as base price
    const hypePool = await fetchUpheavalPoolData(
      UPHEAVAL_POOLS["HYPE"].address,
      "HYPE"
    );
    
    if (!hypePool) {
      logger.error("[Upheaval] Failed to fetch HYPE base price");
      return;
    }
    
    // Calculate WHYPE price in USDT
    const whypePrice = hypePool.token0.symbol === "WHYPE" 
      ? hypePool.price 
      : 1 / hypePool.price;
    
    upheavalPrices.set("HYPE", { ...hypePool, priceInUSDT: whypePrice });
    logger.info(`[Upheaval] HYPE price: $${whypePrice.toFixed(6)}`);
    
    // Fetch other tokens
    for (const token of ["PUP", "UPHL"]) {
      const poolConfig = UPHEAVAL_POOLS[token];
      const poolData = await fetchUpheavalPoolData(poolConfig.address, token);
      
      if (!poolData) continue;
      
      // Calculate price in WHYPE, then convert to USDT
      const priceInWhype = poolData.token1.symbol === "WHYPE" 
        ? poolData.price 
        : 1 / poolData.price;
      
      const priceInUSDT = priceInWhype * whypePrice;
      
      upheavalPrices.set(token, { ...poolData, priceInUSDT });
      logger.info(`[Upheaval] ${token} price: $${priceInUSDT.toFixed(6)}`);
    }
    
  } catch (error) {
    logger.error("[Upheaval] Price update failed:", error);
  }
}

// ==================== Hyperliquid Data ====================
async function fetchAllMids(): Promise<Record<string, string>> {
  const res = await fetch("https://api.hyperliquid.xyz/info", {
    method: "POST",
    body: JSON.stringify({ type: "allMids" }),
    headers: { "Content-Type": "application/json" }
  });
  if (!res.ok) throw new Error("allMids fetch failed");
  return await res.json();
}

async function updateTargetMidPricesOptimized(): Promise<void> {
  try {
    const allMids = await fetchAllMids();
    TARGET_SYMBOLS.forEach(token => {
      const displaySymbol = `${token}-USDT`;
      const internalSymbol = reverseLookup.get(displaySymbol);
      if (internalSymbol && allMids[internalSymbol]) {
        targetMidsCache.set(internalSymbol, allMids[internalSymbol]);
      }
    });
  } catch (error) {
    logger.error("[HL] Mid price update failed:", error);
  }
}

async function initSpotMetadata(): Promise<string[]> {
  try {
    const response = await fetch("https://api.hyperliquid.xyz/info", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type: "spotMeta" }),
    });
    
    if (!response.ok) return [];
    
    const spotMetadata: any = await response.json();
    const tokenMap = new Map<number, string>();
    
    spotMetadata.tokens.forEach((token: any) => 
      tokenMap.set(token.index, token.name)
    );

    const spotCoins: string[] = [];
    spotMetadata.universe.forEach((pair: any) => {
      if (pair.name.startsWith("@")) {
        spotCoins.push(pair.name);
        const tokenNames = pair.tokens.map(
          (tokenIndex: number) => tokenMap.get(tokenIndex) || `Token${tokenIndex}`
        );
        if (tokenNames.length === 2) {
          const displayName = `${tokenNames[0]}-${tokenNames[1]}`;
          spotSymbolMapping.set(pair.name, displayName);
          reverseLookup.set(displayName, pair.name);
        }
      }
    });
    
    return spotCoins;
  } catch (error) {
    logger.error("[HL] Spot metadata load failed:", error);
    return [];
  }
}

function processOrderbookData(coin: string, l2Data: any): CoinOrderbook | null {
  if (!isRunning) return null;
  
  try {
    const displaySymbol = spotSymbolMapping.get(coin) || coin;
    let midPrice = targetMidsCache.get(coin);
    
    const bids = l2Data.levels[0] || [];
    const asks = l2Data.levels[1] || [];

    if (bids.length === 0 || asks.length === 0) return null;

    const bestBid = parseFloat(bids[0].px);
    const bestAsk = parseFloat(asks[0].px);
    
    if (!midPrice || midPrice === "0") {
      midPrice = ((bestBid + bestAsk) / 2).toString();
    }
    
    const spread = bestAsk - bestBid;
    const spreadPercentage = bestBid > 0 ? ((spread / bestBid) * 100).toFixed(4) : "0";
    const timestamp = new Date().toISOString();

    const maxLevels = 5;
    let bidCumulative = 0;
    const topBids: OrderbookLevel[] = bids.slice(0, maxLevels).map((bid: any, idx: number) => {
      bidCumulative += parseFloat(bid.sz);
      return {
        level: idx + 1,
        price: bid.px,
        size: bid.sz,
        cumulative: bidCumulative.toFixed(4),
        timestamp,
      };
    });

    let askCumulative = 0;
    const topAsks: OrderbookLevel[] = asks.slice(0, maxLevels).map((ask: any, idx: number) => {
      askCumulative += parseFloat(ask.sz);
      return {
        level: idx + 1,
        price: ask.px,
        size: ask.sz,
        cumulative: askCumulative.toFixed(4),
        timestamp,
      };
    });

    return {
      symbol: coin,
      displaySymbol,
      lastUpdate: timestamp,
      midPrice,
      spread: spread.toFixed(8),
      spreadPercentage: `${spreadPercentage}%`,
      bestBid,
      bestAsk,
      bids: topBids,
      asks: topAsks,
    };
  } catch (error) {
    logger.error("[HL] Orderbook processing error:", error);
    return null;
  }
}

// ==================== Price Impact Calculation ====================
function calculateUpheavalPriceImpact(
  poolData: UpheavalPoolData,
  side: "BUY" | "SELL",
  targetAmountUSD: number
): { executionPrice: number; priceImpact: number; feasible: boolean } {
  try {
    const token0 = new Token(
      CHAIN_ID,
      poolData.token0.address,
      poolData.token0.decimals,
      poolData.token0.symbol
    );
    const token1 = new Token(
      CHAIN_ID,
      poolData.token1.address,
      poolData.token1.decimals,
      poolData.token1.symbol
    );

    const pool = new Pool(
      token0,
      token1,
      poolData.feeTier,
      JSBI.BigInt(poolData.sqrtPriceX96),
      JSBI.BigInt(poolData.liquidity),
      poolData.tick
    );

    // Determine input/output tokens based on side
    const isToken0USDT = poolData.token0.symbol === "USDT0" || poolData.token0.symbol === "USDT";
    const quoteToken = isToken0USDT ? token0 : token1;
    const baseToken = isToken0USDT ? token1 : token0;

    // Calculate amount in
    const amountInUSD = side === "BUY" ? targetAmountUSD : targetAmountUSD / poolData.priceInUSDT;
    const amountInRaw = JSBI.BigInt(Math.floor(amountInUSD * 10 ** quoteToken.decimals));
    const amountIn = CurrencyAmount.fromRawAmount(quoteToken, amountInRaw);

    const route = new Route([pool], quoteToken, baseToken);
    const trade = Trade.fromRoute(route, amountIn, TradeType.EXACT_INPUT);

    const executionPrice = parseFloat(trade.executionPrice.toSignificant(12));
    const priceImpact = parseFloat(trade.priceImpact.toSignificant(6));

    return {
      executionPrice: executionPrice * (side === "BUY" ? 1 + UPHEAVAL_FEE_BPS/10000 : 1 - UPHEAVAL_FEE_BPS/10000),
      priceImpact,
      feasible: priceImpact < 5
    };
  } catch (error) {
    logger.error("[Upheaval] Price impact calc error:", error);
    const feePct = UPHEAVAL_FEE_BPS / 10000;
    return {
      executionPrice: poolData.priceInUSDT * (side === "BUY" ? 1 + feePct : 1 - feePct),
      priceImpact: feePct * 100,
      feasible: true
    };
  }
}

function getBestLevelCapacityUSD(orderbook: CoinOrderbook, side: "BUY" | "SELL"): number {
  const level = side === "BUY" ? orderbook.asks[0] : orderbook.bids[0];
  if (!level) return 0;
  const px = parseFloat(level.price);
  const sz = parseFloat(level.size);
  return px * sz * FIRST_LEVEL_FILL_RATIO;
}

// ==================== Arbitrage Analysis ====================
function analyzeAllRoutes(): ArbitrageRoute[] {
  if (!isRunning) return [];

  const routes: ArbitrageRoute[] = [];
  const validOrderbooks = incrementalOrderbooks.getAllValid();

  for (const token of TARGET_SYMBOLS) {
    let hlOrderbook: CoinOrderbook | null = null;
    
    for (const [symbol, orderbook] of validOrderbooks.entries()) {
      const baseToken = orderbook.displaySymbol.split("-")[0];
      if (baseToken === token) {
        hlOrderbook = orderbook;
        break;
      }
    }

    const dexData = upheavalPrices.get(token);

    if (hlOrderbook && dexData) {
      // Route 1: HL Buy -> DEX Sell
      const hlBestAskCapUSD = getBestLevelCapacityUSD(hlOrderbook, "BUY");
      const maxOrderSize = Math.min(hlBestAskCapUSD, CONFIG.MAX_POSITION_USD);
      
      if (maxOrderSize >= CONFIG.MIN_TRADE_USD) {
        const hlBuyPrice = parseFloat(hlOrderbook.asks[0].price);
        const dexImpact = calculateUpheavalPriceImpact(dexData, "SELL", maxOrderSize);
        
        const hlFee = (HYPERLIQUID_FEE_BPS / 10000) * 100;
        const dexFee = (UPHEAVAL_FEE_BPS / 10000) * 100;
        const bridgeFee = BRIDGE_FEE_PCT;
        const gasCostPct = (ESTIMATED_GAS_COST_USD / maxOrderSize) * 100;
        
        const grossReturn = ((dexImpact.executionPrice - hlBuyPrice) / hlBuyPrice) * 100;
        const netProfit = grossReturn - hlFee - dexFee - bridgeFee - gasCostPct - dexImpact.priceImpact;
        
        routes.push({
          token,
          direction: "HL->DEX",
          buyProtocol: "Hyperliquid",
          sellProtocol: "Upheaval",
          buyPrice: hlBuyPrice,
          sellPrice: dexData.priceInUSDT,
          priceGap: ((dexData.priceInUSDT - hlBuyPrice) / hlBuyPrice) * 100,
          optimalAmount: Math.floor(maxOrderSize),
          netProfit,
          estimatedProfitUSD: (maxOrderSize * netProfit) / 100,
          buyPriceImpact: 0,
          sellPriceImpact: dexImpact.priceImpact,
          buyExecutionPrice: hlBuyPrice,
          sellExecutionPrice: dexImpact.executionPrice,
          status: netProfit >= CONFIG.MIN_PROFIT_PCT && dexImpact.feasible ? "PROFITABLE" : "NEGATIVE",
        });
      } else {
        routes.push({
          token,
          direction: "HL->DEX",
          buyProtocol: "Hyperliquid",
          sellProtocol: "Upheaval",
          buyPrice: 0,
          sellPrice: 0,
          priceGap: 0,
          optimalAmount: 0,
          netProfit: -999,
          estimatedProfitUSD: 0,
          buyPriceImpact: 0,
          sellPriceImpact: 0,
          buyExecutionPrice: 0,
          sellExecutionPrice: 0,
          status: "NO_DATA",
        });
      }

      // Route 2: DEX Buy -> HL Sell
      const hlBestBidCapUSD = getBestLevelCapacityUSD(hlOrderbook, "SELL");
      const maxOrderSize2 = Math.min(hlBestBidCapUSD, CONFIG.MAX_POSITION_USD);
      
      if (maxOrderSize2 >= CONFIG.MIN_TRADE_USD) {
        const hlSellPrice = parseFloat(hlOrderbook.bids[0].price);
        const dexImpact = calculateUpheavalPriceImpact(dexData, "BUY", maxOrderSize2);
        
        const hlFee = (HYPERLIQUID_FEE_BPS / 10000) * 100;
        const dexFee = (UPHEAVAL_FEE_BPS / 10000) * 100;
        const bridgeFee = BRIDGE_FEE_PCT;
        const gasCostPct = (ESTIMATED_GAS_COST_USD / maxOrderSize2) * 100;
        
        const grossReturn = ((hlSellPrice - dexImpact.executionPrice) / dexImpact.executionPrice) * 100;
        const netProfit = grossReturn - hlFee - dexFee - bridgeFee - gasCostPct - dexImpact.priceImpact;
        
        routes.push({
          token,
          direction: "DEX->HL",
          buyProtocol: "Upheaval",
          sellProtocol: "Hyperliquid",
          buyPrice: dexData.priceInUSDT,
          sellPrice: hlSellPrice,
          priceGap: ((hlSellPrice - dexData.priceInUSDT) / dexData.priceInUSDT) * 100,
          optimalAmount: Math.floor(maxOrderSize2),
          netProfit,
          estimatedProfitUSD: (maxOrderSize2 * netProfit) / 100,
          buyPriceImpact: dexImpact.priceImpact,
          sellPriceImpact: 0,
          buyExecutionPrice: dexImpact.executionPrice,
          sellExecutionPrice: hlSellPrice,
          status: netProfit >= CONFIG.MIN_PROFIT_PCT && dexImpact.feasible ? "PROFITABLE" : "NEGATIVE",
        });
      } else {
        routes.push({
          token,
          direction: "DEX->HL",
          buyProtocol: "Upheaval",
          sellProtocol: "Hyperliquid",
          buyPrice: 0,
          sellPrice: 0,
          priceGap: 0,
          optimalAmount: 0,
          netProfit: -999,
          estimatedProfitUSD: 0,
          buyPriceImpact: 0,
          sellPriceImpact: 0,
          buyExecutionPrice: 0,
          sellExecutionPrice: 0,
          status: "NO_DATA",
        });
      }
    } else {
      routes.push(
        {
          token,
          direction: "HL->DEX",
          buyProtocol: "Hyperliquid",
          sellProtocol: "Upheaval",
          buyPrice: 0,
          sellPrice: 0,
          priceGap: 0,
          optimalAmount: 0,
          netProfit: -999,
          estimatedProfitUSD: 0,
          buyPriceImpact: 0,
          sellPriceImpact: 0,
          buyExecutionPrice: 0,
          sellExecutionPrice: 0,
          status: "NO_DATA",
        },
        {
          token,
          direction: "DEX->HL",
          buyProtocol: "Upheaval",
          sellProtocol: "Hyperliquid",
          buyPrice: 0,
          sellPrice: 0,
          priceGap: 0,
          optimalAmount: 0,
          netProfit: -999,
          estimatedProfitUSD: 0,
          buyPriceImpact: 0,
          sellPriceImpact: 0,
          buyExecutionPrice: 0,
          sellExecutionPrice: 0,
          status: "NO_DATA",
        }
      );
    }
  }

  return routes;
}

// ==================== Trade Execution ====================
async function executeSpotMarketOrder(
  token: string, 
  isBuy: boolean, 
  sizeUSD: number
): Promise<any> {
  try {
    if (sizeUSD < CONFIG.MIN_TRADE_USD) {
      throw new Error(`Order size ${sizeUSD} USD below minimum $${CONFIG.MIN_TRADE_USD}`);
    }
    
    const spotMeta = await publicClient.spotMeta();
    const displaySymbol = `${token}-USDT`;
    const internalSymbol = reverseLookup.get(displaySymbol);
    
    if (!internalSymbol) {
      throw new Error(`${displaySymbol} not found`);
    }
    
    const pair = spotMeta.universe.find((p: any) => p.name === internalSymbol);
    if (!pair) {
      throw new Error(`Pair ${internalSymbol} not found`);
    }
    
    const assetId = 10000 + pair.index;
    
    const allMids = await fetchAllMids();
    const currentPrice = parseFloat(allMids[internalSymbol] || "0");
    if (currentPrice === 0) {
      throw new Error(`No price for ${token}`);
    }
    
    const tokenInfo = spotMeta.tokens.find((t: any) => t.name === token);
    const szDecimals = tokenInfo?.szDecimals || 8;
    
    let orderSize = sizeUSD / currentPrice;
    const lotSize = Math.pow(10, -szDecimals);
    orderSize = Math.floor(orderSize / lotSize) * lotSize;
    
    if (orderSize < lotSize) {
      throw new Error(`Calculated size ${orderSize} below lot size ${lotSize}`);
    }
    
    const result = await exchangeClient.order({
      orders: [{
        a: assetId,
        b: isBuy,
        p: currentPrice.toFixed(szDecimals),
        s: orderSize.toFixed(szDecimals),
        r: false,
        t: { limit: { tif: "Ioc" } }
      }],
      grouping: 'na'
    });
    
    if (result.status === 'ok') {
      const status = result.response.data.statuses[0];
      logger.info(`[HL] ${isBuy ? 'BUY' : 'SELL'} ${token}: ${orderSize.toFixed(szDecimals)} @ ${currentPrice.toFixed(4)}`);
      return status;
    } else {
      throw new Error(`Order failed: ${JSON.stringify(result)}`);
    }
    
  } catch (error) {
    logger.error(`[HL] Order error:`, error);
    throw error;
  }
}

async function transferToHyperEVM(token: string, amountTokens: number): Promise<void> {
  try {
    const spotMeta = await publicClient.spotMeta();
    const tokenMeta = spotMeta.tokens.find((t: any) => t.name === token);
    if (!tokenMeta) {
      throw new Error(`Token ${token} not found`);
    }
    
    const tokenId = tokenMeta.tokenId;
    const tokenStr = `${token}:${tokenId}`;
    
    const result = await exchangeClient.spotSend({
      destination: evmWallet.address,
      token: tokenStr,
      amount: amountTokens.toFixed(tokenMeta.szDecimals)
    });
    
    if (result.status !== 'ok') {
      throw new Error(`Bridge failed: ${JSON.stringify(result)}`);
    }
    
    logger.info(`[Bridge] Core→EVM: ${amountTokens.toFixed(4)} ${token}`);
    await new Promise(resolve => setTimeout(resolve, CONFIG.BRIDGE_DELAY_MS));
    
  } catch (error) {
    logger.error(`[Bridge] Core→EVM failed:`, error);
    throw error;
  }
}

async function transferFromHyperEVM(
  tokenAddress: string, 
  amount: string, 
  decimals: number
): Promise<void> {
  try {
    const systemAddress = await getSystemAddressForToken(tokenAddress);
    
    const erc20 = new ethers.Contract(
      tokenAddress,
      ['function transfer(address,uint256) returns(bool)'],
      evmWallet
    );
    
    const amountWei = ethers.parseUnits(amount, decimals);
    const feeData = await evmProvider.getFeeData();
    
    const tx = await erc20.transfer(systemAddress, amountWei, { 
      gasLimit: 100000n,
      maxFeePerGas: feeData.maxFeePerGas,
      maxPriorityFeePerGas: feeData.maxPriorityFeePerGas
    });
    
    await tx.wait();
    
    logger.info(`[Bridge] EVM→Core: ${amount} tokens, tx: ${tx.hash}`);
    await new Promise(resolve => setTimeout(resolve, CONFIG.BRIDGE_DELAY_MS));
    
  } catch (error) {
    logger.error(`[Bridge] EVM→Core failed:`, error);
    throw error;
  }
}

async function executeUpheavalSwap(
  tokenIn: string,
  tokenOut: string,
  amountIn: string,
  decimalsIn: number,
  poolAddress: string
): Promise<string> {
  try {
    const poolData = upheavalPrices.get(TARGET_SYMBOLS.find(t => TOKEN_ADDRESSES[t] === tokenOut) || "");
    if (!poolData) throw new Error("Pool data not found");

    const token0 = new Token(CHAIN_ID, poolData.token0.address, poolData.token0.decimals);
    const token1 = new Token(CHAIN_ID, poolData.token1.address, poolData.token1.decimals);
    const inputToken = tokenIn.toLowerCase() === token0.address.toLowerCase() ? token0 : token1;
    const outputToken = tokenIn.toLowerCase() === token0.address.toLowerCase() ? token1 : token0;

    const pool = new Pool(
      token0,
      token1,
      poolData.feeTier,
      JSBI.BigInt(poolData.sqrtPriceX96),
      JSBI.BigInt(poolData.liquidity),
      poolData.tick
    );

    const amountInWei = ethers.parseUnits(amountIn, decimalsIn);
    const currencyAmount = CurrencyAmount.fromRawAmount(inputToken, JSBI.BigInt(amountInWei.toString()));
    
    const route = new Route([pool], inputToken, outputToken);
    const trade = Trade.fromRoute(route, currencyAmount, TradeType.EXACT_INPUT);
    
    const slippageTolerance = new Percent(Math.floor(CONFIG.SLIPPAGE_TOLERANCE * 100), 10000);
    const deadline = Math.floor(Date.now() / 1000) + 1800;

    const swapOptions: SwapOptions = {
      slippageTolerance,
      deadline,
      recipient: evmWallet.address,
    };

    const methodParameters = SwapRouter.swapCallParameters([trade], swapOptions);

    // Approve if needed
    const erc20 = new ethers.Contract(
      tokenIn,
      ['function allowance(address,address) view returns (uint256)', 'function approve(address,uint256) returns(bool)'],
      evmWallet
    );
    
    const allowance = await erc20.allowance(evmWallet.address, UPHEAVAL_ROUTER);
    
    if (allowance < amountInWei) {
      logger.info("[Upheaval] Approving...");
      const approveTx = await erc20.approve(UPHEAVAL_ROUTER, ethers.MaxUint256);
      await approveTx.wait();
      logger.info("[Upheaval] Approval confirmed");
    }
    
    // Execute swap
    const feeData = await evmProvider.getFeeData();
    
    const tx = await evmWallet.sendTransaction({
      to: UPHEAVAL_ROUTER,
      data: methodParameters.calldata,
      value: methodParameters.value,
      gasLimit: 300000n,
      maxFeePerGas: feeData.maxFeePerGas,
      maxPriorityFeePerGas: feeData.maxPriorityFeePerGas
    });
    
    logger.info(`[Upheaval] Swap tx sent: ${tx.hash}`);
    const receipt = await tx.wait();
    
    if (!receipt || receipt.status === 0) {
      throw new Error(`Swap transaction failed: ${tx.hash}`);
    }
    
    logger.info(`[Upheaval] Swap confirmed in block ${receipt.blockNumber}`);
    return tx.hash;
    
  } catch (error: any) {
    logger.error("[Upheaval] Swap failed:", error.message);
    throw error;
  }
}

// ==================== Arbitrage Execution ====================
async function executeArbitrageRoute(route: ArbitrageRoute): Promise<void> {
  if (route.status !== 'PROFITABLE' || route.netProfit < CONFIG.MIN_PROFIT_PCT) {
    return;
  }
  
  const release = await tokenMutex.acquire(route.token);
  const amount = Math.min(route.optimalAmount, CONFIG.MAX_POSITION_USD);
  
  logger.info(`[ARB] Executing: ${route.token} ${route.direction}, Amount: $${amount}, Expected: ${route.netProfit.toFixed(2)}%`);
  
  let success = false;
  let error = "";
  
  try {
    await circuitBreaker.execute(async () => {
      if (route.direction === 'HL->DEX') {
        // 1. Buy on Hyperliquid
        await executeSpotMarketOrder(route.token, true, amount);
        
        // 2. Bridge to EVM
        const tokenAmount = amount / route.buyExecutionPrice;
        await transferToHyperEVM(route.token, tokenAmount);
        
        // 3. Swap on Upheaval
        const tokenAddress = TOKEN_ADDRESSES[route.token];
        const decimals = TOKEN_DECIMALS[route.token];
        const poolAddress = UPHEAVAL_POOLS[route.token].address;
        
        await executeUpheavalSwap(
          tokenAddress,
          USDT0_ADDRESS,
          tokenAmount.toFixed(decimals),
          decimals,
          poolAddress
        );
        
        // 4. Bridge USDT back
        const erc20 = new ethers.Contract(
          USDT0_ADDRESS,
          ['function balanceOf(address) view returns (uint256)'],
          evmProvider
        );
        const usdtBalance = await erc20.balanceOf(evmWallet.address);
        const usdtAmount = ethers.formatUnits(usdtBalance, 6);
        await transferFromHyperEVM(USDT0_ADDRESS, usdtAmount, 6);
        
        success = true;
        
      } else { // DEX->HL
        // 1. Swap on Upheaval
        const poolAddress = UPHEAVAL_POOLS[route.token].address;
        await executeUpheavalSwap(
          USDT0_ADDRESS,
          TOKEN_ADDRESSES[route.token],
          amount.toFixed(6),
          6,
          poolAddress
        );
        
        // 2. Bridge to Core
        const tokenAddress = TOKEN_ADDRESSES[route.token];
        const decimals = TOKEN_DECIMALS[route.token];
        const erc20 = new ethers.Contract(
          tokenAddress,
          ['function balanceOf(address) view returns (uint256)'],
          evmProvider
        );
        const tokenBalance = await erc20.balanceOf(evmWallet.address);
        const tokenAmount = ethers.formatUnits(tokenBalance, decimals);
        
        await transferFromHyperEVM(tokenAddress, tokenAmount, decimals);
        
        // 3. Sell on Hyperliquid
        const sellAmount = amount * FIRST_LEVEL_FILL_RATIO;
        await executeSpotMarketOrder(route.token, false, sellAmount);
        
        success = true;
      }
      
      logger.info(`[ARB] ✅ Completed: ${route.token}`);
    }, `arbitrage_${route.token}`);
    
  } catch (err: any) {
    error = err.message;
    logger.error(`[ARB] ❌ Failed:`, err);
  } finally {
    await appendToCSV([route], success, error);
    release();
  }
}

// ==================== Real-time Display ====================
function displayRealTimeRoutes(routes: ArbitrageRoute[]): void {
  process.stdout.write('\x1Bc');
  
  const now = new Date().toLocaleTimeString();
  
  console.log("=".repeat(100));
  console.log(`Hyperliquid ↔ Upheaval Arbitrage Bot - ${now}`);
  console.log(`Auto: ${CONFIG.AUTO_EXECUTE ? 'ON' : 'OFF'} | Min: ${CONFIG.MIN_PROFIT_PCT}% | Max: $${CONFIG.MAX_POSITION_USD}`);
  console.log("=".repeat(100));
  
  console.log(
    "Token".padEnd(6) +
    "Dir".padEnd(8) +
    "Buy".padEnd(10) +
    "Sell".padEnd(10) +
    "Gap%".padEnd(9) +
    "Net%".padEnd(9) +
    "Amount".padEnd(10) +
    "Status"
  );
  console.log("-".repeat(100));
  
  routes.forEach(route => {
    const color = route.status === "PROFITABLE" ? "\x1b[32m" : route.status === "NEGATIVE" ? "\x1b[31m" : "\x1b[33m";
    const reset = "\x1b[0m";
    
    console.log(
      route.token.padEnd(6) +
      route.direction.slice(0,6).padEnd(8) +
      `${route.buyPrice.toFixed(4)}`.padEnd(10) +
      `${route.sellPrice.toFixed(4)}`.padEnd(10) +
      `${route.priceGap.toFixed(2)}%`.padEnd(9) +
      color + `${route.netProfit > -999 ? route.netProfit.toFixed(2) : 'N/A'}%`.padEnd(9) + reset +
      `$${route.optimalAmount.toFixed(10)}`.padEnd(10) +
      color + route.status.slice(0,3) + reset
    );
  });
  
  console.log("=".repeat(100));
  
  const profitable = routes.filter(r => r.status === "PROFITABLE");
  console.log(`Profitable: ${profitable.length}`);
  
  if (profitable.length > 0) {
    console.log(`
🔥 Top opportunities:`);
    profitable.slice(0, 3).forEach(r => {
      console.log(`  ${r.token} ${r.direction}: ${r.netProfit.toFixed(2)}% ($${r.estimatedProfitUSD.toFixed(2)})`);
    });
  }
  
  console.log("=".repeat(100));
}

// ==================== WebSocket ====================
async function connectWebSocket(spotCoins: string[]): Promise<void> {
  const ws = new WebSocket("wss://api.hyperliquid.xyz/ws");
  let lastAnalysisTime = Date.now();
  
  ws.binaryType = "arraybuffer";

  ws.on("open", () => {
    logger.info("[WS] Connected to Hyperliquid");
    
    const targetCoins = spotCoins.filter(coin => {
      const displaySymbol = spotSymbolMapping.get(coin);
      if (!displaySymbol) return false;
      const baseToken = displaySymbol.split("-")[0];
      return TARGET_SYMBOLS.includes(baseToken);
    });
    
    logger.info(`[WS] Subscribing to ${targetCoins.length} pairs`);
    
    targetCoins.forEach((coin) => {
      ws.send(JSON.stringify({ 
        method: "subscribe", 
        subscription: { type: "l2Book", coin } 
      }));
    });
  });

  ws.on("message", async (data: Buffer) => {
    if (!isRunning) return;
    
    try {
      const message = JSON.parse(data.toString('utf8'));
      
      if (message.channel === "l2Book" && message.data) {
        const coin = message.data.coin;
        if (!coin.startsWith("@")) return;

        const coinData = processOrderbookData(coin, message.data);
        if (coinData) {
          incrementalOrderbooks.set(coinData.symbol, coinData);
          
          setImmediate(async () => {
            if (!isRunning) return;
            
            const validOrderbooks = incrementalOrderbooks.getAllValid();
            if (validOrderbooks.size > 0 && upheavalPrices.size > 0) {
              const routes = analyzeAllRoutes();
              displayRealTimeRoutes(routes);
              await appendToCSV(routes, false);
              
              if (CONFIG.AUTO_EXECUTE) {
                const bestRoute = routes
                  .filter(r => r.status === 'PROFITABLE' && r.netProfit >= CONFIG.MIN_PROFIT_PCT)
                  .sort((a, b) => b.netProfit - a.netProfit)[0];
                
                if (bestRoute) {
                  await executeArbitrageRoute(bestRoute);
                }
              }
            }
          });
        }
      }
    } catch (error) {
      logger.error("[WS] Message processing error:", error);
    }
  });

  ws.on("close", () => {
    logger.warn("[WS] Connection closed");
    if (isRunning) {
      logger.info("[WS] Reconnecting in 5 seconds...");
      setTimeout(() => connectWebSocket(spotCoins), 5000);
    }
  });

  ws.on("error", (error) => {
    logger.error("[WS] Error:", error);
  });
}

// ==================== Graceful Shutdown ====================
function setupGracefulShutdown(): void {
  const shutdown = async (signal: string) => {
    logger.info(`[Shutdown] Received signal: ${signal}`);
    isRunning = false;
    
    if (dexUpdateInterval) clearInterval(dexUpdateInterval);
    if (ttlSweepInterval) clearInterval(ttlSweepInterval);
    if (healthCheckInterval) clearInterval(healthCheckInterval);
    
    if (csvWriteStream) {
      await new Promise<void>((resolve) => {
        csvWriteStream!.end(() => {
          logger.info("[Shutdown] CSV file saved");
          resolve();
        });
      });
    }
    
    logger.info("[Shutdown] Complete ✓");
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on('unhandledRejection', (reason, promise) => {
    logger.error('[Process] Unhandled Rejection:', reason);
  });
  
  process.on('uncaughtException', (error) => {
    logger.error('[Process] Uncaught Exception:', error);
    shutdown("UNCAUGHT_EXCEPTION");
  });
}

// ==================== Main Function ====================
async function run(): Promise<void> {
  console.log('='.repeat(80));
  console.log('🚀 Hyperliquid ↔ Upheaval Arbitrage Bot v3.0');
  console.log('='.repeat(80));
  console.log(`📊 Max Position: $${CONFIG.MAX_POSITION_USD}`);
  console.log(`💰 Min Profit: ${CONFIG.MIN_PROFIT_PCT}%`);
  console.log(`🤖 Auto Execute: ${CONFIG.AUTO_EXECUTE ? '✅ ON' : '❌ OFF'}`);
  console.log(`⛽ Max Gas Price: ${CONFIG.MAX_GAS_PRICE_GWEI} gwei`);
  console.log(`📉 Slippage Tolerance: ${CONFIG.SLIPPAGE_TOLERANCE}%`);
  console.log('='.repeat(80));

  setupGracefulShutdown();

  try {
    // 1. Create log directory
    await fs.mkdir('logs', { recursive: true });
    logger.info("[Init] Log directory created");
    
    // 2. Create output directory
    outputDir = path.join(process.cwd(), "arbitrage-output");
    await fs.mkdir(outputDir, { recursive: true });
    logger.info("[Init] Output directory created");

    // 3. Initialize CSV
    await initializeCSVStream();
    logger.info("[Init] CSV stream initialized ✓");

    // 4. Initialize RPC provider
    logger.info("[Init] Initializing RPC provider...");
    await initializeProvider();
    logger.info("[Init] RPC provider connected ✓");

    // 5. Health check
    logger.info("[Init] Performing initial health check...");
    const healthy = await performHealthCheck();
    if (!healthy) {
      throw new Error("Initial health check failed");
    }
    logger.info("[Init] Health check passed ✓");

    // 6. Balance validation
    logger.info("[Init] Validating balances...");
    const balancesOk = await validateBalances();
    if (!balancesOk) {
      logger.warn("[Init] ⚠️  Low balances detected, but continuing...");
    } else {
      logger.info("[Init] Balances validated ✓");
    }

    // 7. Load spot metadata
    logger.info("[Init] Loading Hyperliquid spot metadata...");
    const allSpotCoins = await initSpotMetadata();
    if (allSpotCoins.length === 0) {
      throw new Error("Failed to load spot pairs");
    }

    const targetSpotCoins = allSpotCoins.filter((internalSymbol) => {
      const displayName = spotSymbolMapping.get(internalSymbol);
      if (!displayName) return false;
      const baseSymbol = displayName.split("-")[0];
      return TARGET_SYMBOLS.includes(baseSymbol);
    });

    logger.info(`[Init] Target pairs loaded: ${targetSpotCoins.map(s => spotSymbolMapping.get(s)).join(", ")}`);

    // 8. Load initial prices
    logger.info("[Init] Loading initial prices from Upheaval...");
    await Promise.all([
      updateTargetMidPricesOptimized(),
      updateUpheavalPrices()
    ]);
    logger.info(`[Init] Prices loaded: HL=${targetMidsCache.size}, Upheaval=${upheavalPrices.size} ✓`);

    // Verify we have price data
    if (upheavalPrices.size === 0) {
      throw new Error("Failed to load Upheaval prices - check subgraph connectivity");
    }

    // 9. Start periodic updates
    logger.info("[Init] Starting periodic tasks...");
    
    dexUpdateInterval = setInterval(async () => {
      if (!isRunning) return;
      try {
        await updateUpheavalPrices();
      } catch (error) {
        logger.error("[Task] Upheaval price update failed:", error);
      }
    }, CONFIG.DEX_UPDATE_INTERVAL_MS);

    ttlSweepInterval = setInterval(() => {
      if (!isRunning) return;
      incrementalOrderbooks.sweep();
    }, 30000);

    healthCheckInterval = setInterval(async () => {
      if (!isRunning) return;
      const healthy = await performHealthCheck();
      if (!healthy) {
        logger.error("[Health] Check failed - stopping bot for safety");
        isRunning = false;
      }
    }, CONFIG.HEALTH_CHECK_INTERVAL_MS);

    logger.info("[Init] Periodic tasks started ✓");

    // 10. Connect to WebSocket
    logger.info("[Init] Connecting to Hyperliquid WebSocket...");
    await connectWebSocket(targetSpotCoins);
    
    logger.info("" + "=".repeat(80));
    logger.info("✅ Bot is now running! Press Ctrl+C to stop.");
    logger.info("📈 Monitoring arbitrage opportunities...");
    logger.info("=".repeat(80) + "");
    
  } catch (error) {
    logger.error("[Init] ❌ Initialization failed:", error);
    process.exit(1);
  }
}

// ==================== Entry Point ====================
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

if (path.resolve(process.argv[1]) === path.resolve(__filename)) {
  run().catch((error) => {
    logger.error("[Runtime] Critical error:", error);
    process.exit(1);
  });
}

export { 
  run, 
  analyzeAllRoutes, 
  executeArbitrageRoute,
  performHealthCheck,
  validateBalances,
  updateUpheavalPrices
};
