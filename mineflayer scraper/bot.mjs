// bot.js
import mineflayer from "mineflayer";
import chalk from "chalk";
import { pathToFileURL } from 'url'
import { createRequire } from 'module'
import { getChatEvents } from './utils/getChatEvents.mjs'

// Initialize bot state
let botState = {
  currentLobbyStatus: 'UNKNOWN', // 'UNKNOWN', 'IN_LOBBY', 'IN_GAME', 'CHANGING_LOBBY'
  lastLocrawTime: 0,
  locrawTimeoutId: null,
  lobbyChangeTimeoutId: null,
  expectedLobby: null,
  // nextLobbyIndex is authoritative target for /swaplobby; set on spawn to 1
};

const ONLY_CHAT_SCRAPING = false; // true => only chat; false => tab + chat
let scrapingMode = ONLY_CHAT_SCRAPING ? 'chat' : 'both';

import { getLocation } from './utils/getLocation.mjs';
import { writeFile, readFile, appendFile } from 'fs/promises';
import path from 'path';
let currentLobby = null;

// Paths
const scrapedNamesFile = path.join('..', 'player_names', 'scraped_names_to_process.txt');
const mvpPlusPlusFile = path.join('.', 'mvp_plus_plus_joins.txt');

// Load account details
const ACCOUNT = JSON.parse(
  await readFile(new URL('./secrets/ACCOUNT.json', import.meta.url))
);

// Bot args
let botArgs = { host: 'mc.hypixel.net', version: '1.8.9' };

// Readline for console input
import readline from 'readline';
const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

// Runtime containers
let bots = [];
let botNames = [];
let MASK = {};
let params = { showClickEvents: true, showHoverEvents: false, showName: false, showMask: false };
let currentTask = null;
let defaultLobby = "duels";

// Feature Toggles
const ENABLE_NPC_FILTERING = false;

// Debounce map for updatePlayerList per bot
const lastUpdateTs = new WeakMap();
const UPDATE_COOLDOWN_MS = 5000;

// Lobby cycling configuration
const LOBBY_STAY_MS = 15000;
const SWAP_WAIT_TIMEOUT_MS = 15000;
const SWAP_RETRY_BACKOFF_MS = 15000;
const RATE_LIMIT_BACKOFF_MS = 15000;
const MAX_LOBBY = 9;

// MVP++ Join Deduplication Cache
let recentMvpPlusPlusJoins = new Set();
const RECENT_MVP_PLUS_PLUS_CACHE_DURATION_MS = 30000; // 5 minutes

// ---------- Global rate limiting across all bots (ADAPTIVE) ----------
let lastGlobalCommandTime = 0;
const GLOBAL_COMMAND_GAP_MS = 5000;   // base floor
let DYNAMIC_GLOBAL_GAP_MS = GLOBAL_COMMAND_GAP_MS;
let consecutiveSuccesses = 0;

async function sendCommandWithGlobalRateLimit(bot, command) {
  const now = Date.now();
  const delta = now - lastGlobalCommandTime;
  const gap = DYNAMIC_GLOBAL_GAP_MS;
  if (delta < gap) {
    const wait = gap - delta;
    console.log(chalk.cyan(`Global rate limit: waiting ${wait}ms before command for ${bot.username}`));
    await new Promise(r => setTimeout(r, wait));
  }
  lastGlobalCommandTime = Date.now();
  sendHighlighted(bot, command);
}

function noteRateLimitHit() {
  // ramp up more aggressively, cap at 15s
  DYNAMIC_GLOBAL_GAP_MS = Math.min(Math.round(DYNAMIC_GLOBAL_GAP_MS * 1.4) + 500, 15000);
  consecutiveSuccesses = 0;
  console.log(chalk.red(`[RL] Increased global gap to ${DYNAMIC_GLOBAL_GAP_MS}ms`));
}
function noteCommandSuccess() {
  consecutiveSuccesses++;
  if (consecutiveSuccesses >= 3 && DYNAMIC_GLOBAL_GAP_MS > GLOBAL_COMMAND_GAP_MS) {
    DYNAMIC_GLOBAL_GAP_MS = Math.max(DYNAMIC_GLOBAL_GAP_MS - 1000, GLOBAL_COMMAND_GAP_MS);
    consecutiveSuccesses = 0;
    console.log(chalk.green(`[OK] Decreased global gap to ${DYNAMIC_GLOBAL_GAP_MS}ms`));
  }
}

// Per-bot rate limit tracking (exponential backoff)
const botRateLimitCounts = new Map();
function getBackoffTime(botUsername, baseTime = SWAP_RETRY_BACKOFF_MS) {
  const rateLimitCount = botRateLimitCounts.get(botUsername) || 0;
  return Math.min(baseTime * Math.pow(2, rateLimitCount), 300000); // cap 5m
}
function incrementRateLimitCount(botUsername) {
  const current = botRateLimitCounts.get(botUsername) || 0;
  botRateLimitCounts.set(botUsername, current + 1);
  console.log(chalk.yellow(`Rate limit count for ${botUsername}: ${current + 1}`));
}
function resetRateLimitCount(botUsername) {
  if (botRateLimitCounts.has(botUsername)) {
    botRateLimitCounts.set(botUsername, 0);
    console.log(chalk.green(`Reset rate limit count for ${botUsername}`));
  }
}

// ---------- Helpers ----------
function normalizeChatText(raw) {
  return raw.replace(/^[^[\w]+/, '').replace(/\s+<<+.*$/,'').trim();
}
function getCleanDisplayName(displayNameJson) {
  let fullText = '';
  if (displayNameJson.text) fullText += displayNameJson.text;
  if (displayNameJson.extra) {
    for (const part of displayNameJson.extra) if (part.text) fullText += part.text;
  }
  return fullText.replace(/§./g, '');
}
async function appendMvpPlusPlusJoin(ign, bot) {
  if (recentMvpPlusPlusJoins.has(ign)) return;
  const ts = new Date().toLocaleString('en-US', { year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false });
  const lobby = (bot && bot.state.currentLobbyStatus) || 'unknown';
  if (!bot._pendingMvpPlusPlusJoins) bot._pendingMvpPlusPlusJoins = [];
  bot._pendingMvpPlusPlusJoins.push({ ign, ts, lobby });
  console.log(chalk.yellow(`Queued MVP++ join for ${ign} (lobby: ${lobby})`));
  recentMvpPlusPlusJoins.add(ign);
  setTimeout(() => { recentMvpPlusPlusJoins.delete(ign); }, RECENT_MVP_PLUS_PLUS_CACHE_DURATION_MS);
}
async function processPendingMvpPlusPlusJoins(bot) {
  if (bot && bot._pendingMvpPlusPlusJoins && bot._pendingMvpPlusPlusJoins.length > 0) {
    console.log(chalk.cyan(`Processing ${bot._pendingMvpPlusPlusJoins.length} pending MVP++ joins.`));
    let linesToWrite = '';
    for (const join of bot._pendingMvpPlusPlusJoins) {
      linesToWrite += `${join.ts}\t${join.ign}\t${join.lobby}\n`;
    }
    try {
      await appendFile(mvpPlusPlusFile, linesToWrite, 'utf-8');
      bot._pendingMvpPlusPlusJoins = [];
    } catch (err) {
      console.error("Failed to write pending MVP++ joins:", err);
    }
  }
}
async function addPlayerToQueue(playerName, source = 'unknown') {
  if (!playerName) return;
  playerName = playerName.trim();
  if (!/^[A-Za-z0-9_]{3,16}$/.test(playerName)) return;
  if (ENABLE_NPC_FILTERING && /^[a-z0-9]{10}$/.test(playerName)) return;
  if (!await nameExistsInFile(playerName, scrapedNamesFile)) {
    try {
      await appendFile(scrapedNamesFile, playerName + '\n', 'utf-8');
      console.log(`Added ${playerName} to scraped names (from ${source})`);
    } catch (err) {
      console.error(`Error appending to ${scrapedNamesFile}:`, err);
    }
  }
}
async function nameExistsInFile(name, filePath) {
  try {
    const fileContent = await readFile(filePath, 'utf-8');
    return fileContent.includes(name);
  } catch (error) {
    if (error.code === 'ENOENT') return false;
    console.error(`Error reading file ${filePath}:`, error);
    return false;
  }
}
async function maybeUpdatePlayerList(bot) {
  try {
    if (scrapingMode === 'both' || scrapingMode === 'tab') {
      const lastTs = lastUpdateTs.get(bot) || 0;
      const now = Date.now();
      if (now - lastTs < UPDATE_COOLDOWN_MS) return;
      lastUpdateTs.set(bot, now);
      await updatePlayerList(bot);
    }
  } catch (e) { console.error("maybeUpdatePlayerList error:", e); }
}

// ---------- Tab / Chat detection ----------
async function updatePlayerList(bot) {
  try {
    if (!bot || !bot.players) return;
    let currentList = Object.values(bot.players).map(p => p && p.username).filter(Boolean);
    const botIndex = currentList.indexOf(bot.username);
    if (botIndex > -1) currentList.splice(botIndex, 1);

    const cleaned = currentList.filter(playerName => {
      if (!playerName) return false;
      if (playerName.includes('[NPC]')) return false;
      return true;
    }).map(playerName => {
      playerName = playerName.replace(/\s*\[.*?\]\s*/g, '');
      playerName = playerName.replace(/[^A-Za-z0-9_]/g, '');
      return playerName;
    }).filter(name => name && name.length >= 3 && name.length <= 16 && /^[A-Za-z0-9_]+$/.test(name));

    const newSet = new Set(cleaned);
    const oldSet = bot._lastTabSet || new Set();

    const added = [];
    for (const name of newSet) if (!oldSet.has(name)) added.push(name);
    const removed = [];
    for (const name of oldSet) if (!newSet.has(name)) removed.push(name);

    bot._lastTabSet = newSet;
    if (!bot._currentlyOnline) bot._currentlyOnline = new Set();

    if (added.length) {
      for (const name of added) {
        bot._currentlyOnline.add(name);
        await addPlayerToQueue(name, 'tab');
      }
    }
    if (removed.length) {
      for (const name of removed) {
        bot._currentlyOnline.delete(name);
      }
    }
  } catch (err) { console.error("updatePlayerList error:", err); }
}

// ---------- Swap helpers ----------
function sendHighlighted(bot, command) {
  try {
    bot.chat(command);
    console.log(chalk.bgBlue.white.bold(` SENT: ${command} `));
  } catch (e) {
    console.log(chalk.bgRed.white.bold(` FAILED TO SEND: ${command} `), e);
  }
}
function extractLobbyNumberFromText(text) {
  let m = text.match(/Lobby\s*#?(\d+)/i);
  if (m && m[1]) return parseInt(m[1], 10);
  m = text.match(/bedwarslobby\s*(\d+)/i);
  if (m && m[1]) return parseInt(m[1], 10);
  return null;
}

// Wait only for the immediate /swaplobby acknowledgement
function waitForSwapAck(bot, timeoutMs = 8000) {
  return new Promise((resolve) => {
    function cleanup() { bot.removeListener('message', onMessage); clearTimeout(timer); }

    function onMessage(jsonMsg) {
      const raw = (jsonMsg?.toString?.() || String(jsonMsg));
      const norm = raw.replace(/§./g, '');

      // Transfer starting → we will send ONE /locraw after a short delay
      if (/Sending you to dynamiclobby/i.test(raw) || /Sending you to dynamiclobby/i.test(norm)) {
        cleanup(); return resolve({ status: 'transferring' });
      }

      // Already in that lobby
      if (/already connected to (?:this|that) server/i.test(raw) || /already connected/i.test(norm)) {
        cleanup(); return resolve({ status: 'already' });
      }

      // Full / unavailable / does not exist
      if (/lobby is currently full|that lobby is full|isn'?t available right now|no such lobby/i.test(norm)) {
        cleanup(); return resolve({ status: 'full_or_unavailable' });
      }
      if (/does not exist/i.test(norm)) {
        cleanup(); return resolve({ status: 'does_not_exist' });
      }

      // AFK, not in lobby, in game
      if (/You are AFK/i.test(norm)) {
        cleanup(); return resolve({ status: 'afk' });
      }
      if (/must be in a lobby|only available in lobbies/i.test(norm)) {
        cleanup(); return resolve({ status: 'need_lobby' });
      }
      if (/cannot use this command while in a game/i.test(norm)) {
        cleanup(); return resolve({ status: 'in_game' });
      }

      // Rate limited
      if (/Woah there, slow down|You are sending commands too fast|rate limited|Slow down/i.test(norm)) {
        cleanup(); return resolve({ status: 'ratelimited' });
      }
    }

    bot.on('message', onMessage);
    const timer = setTimeout(() => { cleanup(); resolve({ status: 'timeout' }); }, timeoutMs);
  });
}

// Wait only for the /locraw JSON result and parse lobby number
function waitForLocrawLobby(bot, timeoutMs = 8000) {
  return new Promise((resolve) => {
    function cleanup() { bot.removeListener('message', onMessage); clearTimeout(timer); }

    function onMessage(jsonMsg) {
      const raw = (jsonMsg?.toString?.() || String(jsonMsg));
      if (!raw || raw[0] !== '{' || !raw.includes('"server"')) return;

      const m = /"lobbyname"\s*:\s*"bedwarslobby(\d+)"/i.exec(raw);
      if (m) {
        cleanup();
        return resolve({ ok: true, lobbyNumber: parseInt(m[1], 10), raw });
      }
    }

    bot.on('message', onMessage);
    const timer = setTimeout(() => { cleanup(); resolve({ ok: false, lobbyNumber: null }); }, timeoutMs);
  });
}

/** One-shot /locraw with getLocation, used at startup (and after /l b) only. */
async function fetchLocraw(bot, timeoutMs = 8000) {
  return new Promise(async (resolve) => {
    let timer;
    function cleanup() { clearTimeout(timer); bot.removeListener('message', onMessage); }
    function onMessage(jsonMsg) {
      try {
        const raw = jsonMsg?.toString?.() || String(jsonMsg);
        if (!raw || raw[0] !== '{' || !raw.includes('"server"')) return;
        const [loc, ok] = getLocation(raw);
        if (!ok) return;
        const bedwars =
          (loc.gametype && String(loc.gametype).toUpperCase() === 'BEDWARS') ||
          (loc.lobbyname && /bedwarslobby/i.test(loc.lobbyname));
        let lobbyNumber = null;
        if (loc.lobbyname) {
          const m = /bedwarslobby(\d+)/i.exec(loc.lobbyname);
          if (m) lobbyNumber = parseInt(m[1], 10);
        }
        cleanup();
        resolve({ ok: true, bedwars, lobbyNumber, loc });
      } catch { /* ignore */ }
    }
    bot.on('message', onMessage);
    await sendCommandWithGlobalRateLimit(bot, "/locraw");
    timer = setTimeout(() => { cleanup(); resolve({ ok: false, bedwars: false, lobbyNumber: null, loc: null }); }, timeoutMs);
  });
}

/** Ensure we are in a BedWars lobby (startup / recovery only). */
async function ensureInBedwarsLobby(bot) {
  let r = await fetchLocraw(bot);
  if (!r.ok || !r.bedwars) {
    await sendCommandWithGlobalRateLimit(bot, "/l b");
    // Confirm once more after /l b
    r = await fetchLocraw(bot);
  }
  bot.state.inBedwarsEnvironment = !!r.bedwars;
  bot.state.currentLobbyStatus = r.lobbyNumber ?? null;
  return !!r.bedwars;
}

/**
 * Attempt to swap to the next lobby using /swaplobby.
 * Exactly **one** /locraw per attempt and only after a transfer ack.
 */
async function attemptSwapToNext(bot) {
  if (!bot || !bot.username || bot._client?.socket?.destroyed) {
    console.log(chalk.yellow(`Bot disconnected, stopping lobby attempts for ${bot?.username || 'unknown'}`));
    return;
  }

  // Authoritative target
  if (typeof bot.state.nextLobbyIndex !== 'number') bot.state.nextLobbyIndex = 1;
  const targetLobby = bot.state.nextLobbyIndex;

  console.log(chalk.cyan(`[${bot.username}] Attempting to swap to lobby: ${targetLobby}`));

  // Per-bot minimum gap between swaps
  const now = Date.now();
  const minGapMs = 15000;
  const last = bot.state.lastSwapTime || 0;
  if (now - last < minGapMs) {
    const waitTime = minGapMs - (now - last);
    console.log(`Waiting ${(waitTime/1000).toFixed(1)}s before next swap for ${bot.username}`);
    await new Promise(r => setTimeout(r, waitTime));
  }

  // 1) Send /swaplobby X
  await sendCommandWithGlobalRateLimit(bot, `/swaplobby ${targetLobby}`);
  bot.state.lastSwapTime = Date.now();

  // 2) Wait for the immediate acknowledgement (NO /locraw yet)
  const ack = await waitForSwapAck(bot, 8000);

  // 3) Handle ack results
  if (ack.status === 'ratelimited') {
    incrementRateLimitCount(bot.username);
    noteRateLimitHit();
    const backoff = getBackoffTime(bot.username, RATE_LIMIT_BACKOFF_MS);
    console.log(chalk.red(`RATE LIMITED — backing off ${Math.round(backoff/1000)}s for ${bot.username}`));
    await new Promise(r => setTimeout(r, backoff));
    return; // do NOT advance, do NOT locraw
  }

  if (ack.status === 'transferring') {
    // Give the server a moment to move us, then do ONE /locraw
    const settle = 1800 + Math.floor(Math.random() * 900); // 1.8–2.7s
    await new Promise(r => setTimeout(r, settle));

    await sendCommandWithGlobalRateLimit(bot, "/locraw");
    const loc = await waitForLocrawLobby(bot, 8000);

    if (loc.ok && loc.lobbyNumber) {
      bot.state.currentLobbyStatus = loc.lobbyNumber;
      console.log(chalk.green(`Swap success; now in lobby ${bot.state.currentLobbyStatus} for ${bot.username}`));
      await processPendingMvpPlusPlusJoins(bot);
      resetRateLimitCount(bot.username);
      noteCommandSuccess();

      // Advance and wrap
      bot.state.nextLobbyIndex = targetLobby + 1;
      if (bot.state.nextLobbyIndex > MAX_LOBBY) bot.state.nextLobbyIndex = 1;

      await new Promise(r => setTimeout(r, 1000));
      if (scrapingMode === 'both' || scrapingMode === 'tab') await updatePlayerList(bot);
      return;
    } else {
      // Couldn't confirm via locraw; don't spam—retry later without advancing
      console.log(chalk.yellow(`Swap transfer detected but /locraw confirmation failed. Will retry later.`));
      await new Promise(r => setTimeout(r, SWAP_RETRY_BACKOFF_MS));
      return;
    }
  }

  if (ack.status === 'already') {
    console.log(chalk.gray(`Already in requested lobby ${targetLobby}. Advancing.`));
    resetRateLimitCount(bot.username);
    noteCommandSuccess();
    bot.state.nextLobbyIndex = targetLobby + 1;
    if (bot.state.nextLobbyIndex > MAX_LOBBY) bot.state.nextLobbyIndex = 1;
    return;
  }

  if (ack.status === 'full_or_unavailable') {
    console.log(chalk.yellow(`Lobby ${targetLobby} full/unavailable — retrying later without advancing.`));
    await new Promise(r => setTimeout(r, SWAP_RETRY_BACKOFF_MS));
    return;
  }

  if (ack.status === 'does_not_exist') {
    console.log(chalk.red(`Lobby ${targetLobby} does not exist — resetting to 1.`));
    bot.state.nextLobbyIndex = 1;
    await new Promise(r => setTimeout(r, 5000));
    return;
  }

  if (ack.status === 'afk' || ack.status === 'need_lobby' || ack.status === 'in_game') {
    console.log(chalk.yellow(`Detected ${ack.status} — sending /l b and resetting cycle.`));
    bot.state.inBedwarsEnvironment = false;
    await sendCommandWithGlobalRateLimit(bot, "/l b");

    // Confirm ONCE after /l b
    const ok = await ensureInBedwarsLobby(bot); // internally issues at most one /locraw (two if needed)
    if (ok) {
      resetRateLimitCount(bot.username);
      noteCommandSuccess();
      bot.state.nextLobbyIndex = 1;
      await new Promise(r => setTimeout(r, 2000));
      if (scrapingMode === 'both' || scrapingMode === 'tab') await updatePlayerList(bot);
    }
    return;
  }

  if (ack.status === 'timeout') {
    console.log(chalk.yellow(`Swap ack timed out — backing off 25s`));
    await new Promise(r => setTimeout(r, 25000));
    return;
  }

  // Fallback
  console.log(chalk.red(`Unhandled swap ack: ${JSON.stringify(ack)}`));
}

// ---------- Bot class ----------
class MCBot {
  constructor(username, password, auth) {
    this.username = username;
    this.password = password;
    this.auth = auth;
    this.host = botArgs.host;
    this.port = botArgs.port;
    this.version = botArgs.version;

    this.botLocation = { server: null, gametype: null, lobbyname: null, map: null };
    this.state = { ...botState };

    this.getChatEvents = getChatEvents;
    this.getLocation = getLocation;

    this.initBot();
  }

  initBot() {
    this.bot = mineflayer.createBot({
      username: this.username,
      password: this.password,
      auth: this.auth,
      host: this.host,
      port: this.port,
      version: this.version,
      hideErrors: true
    });
    this.bot.state = this.state;

    this.initEvents();
    this.listenToUserInput();
  }

  log(...msg) {
    if (params.showName && params.showMask) console.log(this.mask(`[${this.bot.username}] ` + msg[0]));
    else if (params.showName) console.log(`[${this.bot.username}] ` + msg[0]);
    else console.log(msg[0]);
  }
  mask(msg) { for (const key in MASK) msg = msg.replace(new RegExp(key, "gi"), MASK[key]); return msg; }

  listenToUserInput() {
    rl.prompt(true);
    rl.on('line', async (input) => {
      switch (input) {
        case "get location":
          this.log(`Current location: {${this.botLocation.server}${this.botLocation.lobbyname ? `, ${this.botLocation.lobbyname}` : ""}${this.botLocation.gametype ? `, ${this.botLocation.gametype}` : ""}${this.botLocation.map ? `, ${this.botLocation.map}` : ""}}`);
          break;
        case "get task":
          this.log(`Current Task: ${currentTask}`);
          break;
        case "end task":
          currentTask = null;
          break;
        case "/limbo":
          this.bot.chat("§");
          break;
        default:
          this.bot.chat(input);
          break;
      }
    });
  }

  initEvents() {
    this.bot.on('login', async () => {
      let botSocket = this.bot._client.socket;
      this.log(chalk.ansi256(34)(`Logged in to ${botSocket.server ? botSocket.server : botSocket._host}`));
      botNames.push(this.bot.username);
    });

    this.bot.on('end', async (reason) => {
      this.log(chalk.red(`Disconnected: ${reason}`));
      try { clearInterval(this.bot._lobbyInterval); } catch (e) {}
      // Reset all bot state on disconnect
      this.bot.state.inBedwarsEnvironment = false;
      this.bot.state.lobbyCycleActive = false;
      this.bot.state.nextLobbyIndex = undefined;
      this.bot.state.lastSwapTime = 0;
      this.bot.state.currentLobbyStatus = 'UNKNOWN';
      this.bot.state.lastLocrawTime = 0;
      clearTimeout(this.bot.state.locrawTimeoutId);
      clearTimeout(this.bot.state.lobbyChangeTimeoutId);
      this.bot.state.expectedLobby = null;
      if (reason === "disconnect.quitting") return;
      setTimeout(() => this.initBot(), 10000);
    });

    // NOTE: this just logs "Spawned in" (does NOT /locraw)
    this.bot.on('spawn', async () => {
      this.log(chalk.ansi256(46)(`Spawned in`));
      await this.bot.waitForChunksToLoad();
      await this.bot.waitForTicks(12);
      switch (currentTask) { case "task_example": break; default: break; }
    });

    // Tab events
    const knownNpcNames = new Set();
    if (ENABLE_NPC_FILTERING) {
      knownNpcNames.add("eDoorman");
      knownNpcNames.add("eHotelPianist");
    }

    this.bot.on('playerJoined', (player) => {
      if (scrapingMode === 'both' || scrapingMode === 'tab') {
        if (player && player.username) {
          if (player.displayName) {
            const cleanedDisplayName = getCleanDisplayName(player.displayName.json);
            if (ENABLE_NPC_FILTERING && knownNpcNames.has(cleanedDisplayName.toLowerCase())) return;
          }
          if (!this.bot._currentlyOnline) this.bot._currentlyOnline = new Set();
          const cleaned = player.username.replace(/\s*\[.*?\]\s*/g, '').replace(/[^A-Za-z0-9_]/g, '');
          this.bot._currentlyOnline.add(cleaned);
          addPlayerToQueue(cleaned, 'tab');
        }
      }
    });

    this.bot.on('playerLeft', (player) => {
      if (scrapingMode === 'both' || scrapingMode === 'tab') {
        if (player && player.username) {
          if (this.bot._currentlyOnline) {
            const cleaned = player.username.replace(/\s*\[.*?\]\s*/g, '').replace(/[^A-Za-z0-9_]/g, '');
            this.bot._currentlyOnline.delete(cleaned);
          }
        }
      }
    });

    this.bot.on('message', async (jsonMsg) => {
      if (jsonMsg && jsonMsg.extra && jsonMsg.extra.length === 100) return;

      if (scrapingMode === 'both' || scrapingMode === 'chat') {
        const rawText = jsonMsg.toString();
        const normalizedText = normalizeChatText(rawText);

        let ansiText = this.mask(jsonMsg.toAnsi());
        if (params.showName && params.showMask) process.stdout.write(this.mask(`[${this.bot.username}] ${ansiText}`));
        else if (params.showName) process.stdout.write(`[${this.bot.username}] ${ansiText}`);
        else process.stdout.write(ansiText);

        let [messageClickEvents, messageHoverEvents] = this.getChatEvents(jsonMsg);
        let clickEvents = params.showClickEvents && messageClickEvents.length;
        let hoverEvents = params.showHoverEvents && messageHoverEvents.length;
        if (clickEvents && hoverEvents) console.log(messageClickEvents, messageHoverEvents);
        else if (clickEvents) console.log(messageClickEvents);
        else if (hoverEvents) console.log(messageHoverEvents);
        else console.log();

        // Patterns
        const nameRegex = /(?:[\d✫]\s*)?(?:[[A-Z]+\+*]\s*)?([A-Za-z0-9_]{3,16}):/;
        const newPlayerRegex = /(?:[^\\]+\]\s*)?([A-Za-z0-9_]{3,16}) joined the lobby!/;
        const mvpPlusRegex = / \[MVP\+\+\]\s*([A-Za-z0-9_]{3,16}) joined the lobby!/;

        // Track bedwars lobby via locraw JSON (we don't send it here—just parse if present)
        const bedwarsLobbyMatch = /"lobbyname"\s*:\s*"bedwarslobby(\d+)"/i.exec(rawText);
        if (bedwarsLobbyMatch) {
          const lobbyNum = parseInt(bedwarsLobbyMatch[1], 10);
          this.bot.state.currentLobbyStatus = lobbyNum;
          this.bot.state.inBedwarsEnvironment = true;
          clearTimeout(this.bot.state.locrawTimeoutId);
          await processPendingMvpPlusPlusJoins(this.bot);
        }

        const nameMatch = nameRegex.exec(normalizedText) || nameRegex.exec(rawText);
        if (nameMatch) {
          const playerName = nameMatch[1];
          addPlayerToQueue(playerName, 'chat');
        }

        const newPlayerMatch = newPlayerRegex.exec(normalizedText) || newPlayerRegex.exec(rawText);
        if (newPlayerMatch) {
          const playerName = newPlayerMatch[1];
          addPlayerToQueue(playerName, 'chat');

          const mvpMatch = mvpPlusRegex.exec(normalizedText) || mvpPlusRegex.exec(rawText);
          if (mvpMatch && mvpMatch[1]) {
            const ign = mvpMatch[1];
            console.log(chalk.green(`[INFO] MVP++ player ${ign} joined and added to mvp_plus_plus_joins.txt`));
            await appendMvpPlusPlusJoin(ign, this.bot);
          }

          maybeUpdatePlayerList(this.bot);
        }
      }
    });

    this.bot.on('error', async (err) => {
      if (err.code === 'ECONNREFUSED') this.log(`Failed to connect to ${err.address}:${err.port}`);
      else this.log(`Unhandled error: ${err}`);
    });
  }
}

// ---------- Start bots & cycle ----------
async function startBots() {
  // stagger bot creation
  for (let i = 0; i < ACCOUNT.length; i++) {
    let ACC = ACCOUNT[i];
    let newBot = new MCBot(ACC.username, ACC.password, ACC.auth);
    bots.push(newBot);
    MASK[ACC.ign] = `0x_BOT_${String(i + 1).padStart(3, '0')}`;

    console.log(chalk.blue(`Created bot ${i + 1}/${ACCOUNT.length}: ${ACC.username}`));
    if (i < ACCOUNT.length - 1) {
      console.log(chalk.cyan(`Waiting 5s before creating next bot...`));
      await new Promise(r => setTimeout(r, 5000));
    }
  }

  for (const botInstance of bots) {
    const bot = botInstance.bot;
    let bootstrapped = false;

    // IMPORTANT: run main init only once (do not repeat on lobby re-spawns)
    bot.once('spawn', async () => {
      if (bootstrapped) return;
      bootstrapped = true;

      try {
        const randomDelay = Math.random() * 10000;
        console.log(`${botInstance.username} waiting ${(randomDelay/1000).toFixed(1)}s before starting activities...`);
        await new Promise(r => setTimeout(r, randomDelay));

        console.log(`${botInstance.username} spawned. Checking initial location via /locraw...`);
        await bot.waitForChunksToLoad();
        await bot.waitForTicks(12);
        if (!bot._currentlyOnline) bot._currentlyOnline = new Set();

        // Discover once; /l b if needed
        const inBW = await ensureInBedwarsLobby(bot);
        if (!inBW) {
          console.log(chalk.yellow(`Could not confirm BedWars via /locraw for ${bot.username}. Recovery will trigger if needed.`));
        } else {
          console.log(chalk.green(`Confirmed BedWars for ${bot.username}.`));
        }

        // Initialize cycle @ 1
        bot.state.lastSwapTime = 0;
        bot.state.nextLobbyIndex = 1;

        if (!bot.state.lobbyCycleActive) {
          bot.state.lobbyCycleActive = true;

          const cycleStartDelay = Math.random() * 20000;
          console.log(chalk.cyan(`${bot.username} will start lobby cycling in ${(cycleStartDelay/1000).toFixed(1)}s`));

          setTimeout(async () => {
            console.log(chalk.green(`Starting lobby cycling for ${bot.username}`));

            // Start at lobby 1
            try { await attemptSwapToNext(bot); }
            catch (e) { console.error(`Initial swap error for ${bot.username}:`, e); }

            while (bot && !bot._client?.socket?.destroyed && bot.state.lobbyCycleActive) {
              // stay for a bit
              await new Promise(r => setTimeout(r, LOBBY_STAY_MS));

              if (!bot || bot._client?.socket?.destroyed || !bot.state.lobbyCycleActive) break;

              // move to next — (attemptSwapToNext only sends one /locraw after ack)
              try { await attemptSwapToNext(bot); }
              catch (e) { console.error(`Swap error for ${bot.username}:`, e); await new Promise(r => setTimeout(r, 10000)); }

              // safety wrap
              if (bot.state.nextLobbyIndex > MAX_LOBBY) bot.state.nextLobbyIndex = 1;
            }
            console.log(chalk.red(`Lobby cycling stopped for ${bot.username}`));
          }, cycleStartDelay);
        }

        // initial tab sync
        await new Promise(r => setTimeout(r, 3000));
        if (scrapingMode === 'both' || scrapingMode === 'tab') await updatePlayerList(bot);

      } catch (err) {
        console.error(`${botInstance.username} spawn handler error:`, err);
      }
    });

    bot.on('end', (reason) => {
      try { clearInterval(bot._lobbyInterval); } catch (e) {}
      bot.state.inBedwarsEnvironment = false;
      bot.state.lobbyCycleActive = false;
      bot.state.nextLobbyIndex = undefined;
      bot.state.lastSwapTime = 0;
      console.log(chalk.red(`${botInstance.username} disconnected: ${reason}`));
    });
  }
}

// ---------- Startup ----------
console.log(chalk.green("Starting bots with staggered initialization..."));
await startBots();
console.log(chalk.green("All systems started. Bot activities will be staggered to prevent rate limiting."));
