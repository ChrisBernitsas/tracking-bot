
import mineflayer from "mineflayer";
import { readFile } from 'fs/promises';

// Load account details
const ACCOUNT = JSON.parse(
  await readFile(new URL('./secrets/ACCOUNT.json', import.meta.url))
);

// Bot args
const botArgs = {
  host: 'mc.hypixel.net',
  version: '1.8.9',
  auth: ACCOUNT[0].auth,
  username: ACCOUNT[0].username,
  password: ACCOUNT[0].password
};

function createBot() {
  const bot = mineflayer.createBot(botArgs);

  bot.on('login', () => {
    console.log(`Logged in as ${bot.username}`);
  });

  bot.on('spawn', () => {
    console.log('Bot spawned. Starting spammer...');
    setInterval(() => {
      const randomPart = Math.random().toString(36).substring(2, 15);
      const message = `REAL SUPER IS ON ORBIT!!! ${randomPart}`;
      bot.chat(`/msg KDK0 ${message}`);
      console.log(`Sent message to KDK0: ${message}`);
    }, 3000); // Send a message every 3 seconds
  });

  bot.on('error', (err) => {
    console.error('Bot error:', err);
  });

  bot.on('end', (reason) => {
    console.log(`Disconnected: ${reason}`);
    setTimeout(createBot, 5000); // Reconnect after 5 seconds
  });

  return bot;
}

createBot();
