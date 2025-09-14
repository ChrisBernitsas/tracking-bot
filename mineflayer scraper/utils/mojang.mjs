export class MojangAPI {
    static async uuidForName(playerName) {
        const url = `https://api.mojang.com/users/profiles/minecraft/${playerName}`;
        let retryCount = 0;
        const maxRetries = 3;

        while (retryCount < maxRetries) {
            try {
                const response = await fetch(url);
                if (response.status === 200) {
                    const data = await response.json();
                    await this.delay(500); // Add a delay after successful request
                    return data.id;
                } else if (response.status === 404) {
                    await this.delay(500); // Add a delay after unsuccessful request
                    return null; // Player not found
                } else if (response.status === 429) {
                    const retryAfter = response.headers.get('Retry-After') || 10;
                    console.warn(`Rate limited. Retrying after ${retryAfter} seconds...`);
                    await this.delay(retryAfter * 1000);
                    retryCount++;
                } else {
                    throw new Error(`Mojang API error: ${response.status}`);
                }
            } catch (error) {
                console.error("Mojang API request failed:", error);
                retryCount++;
                await this.delay(1000); // Delay before retrying
            }
        }

        console.error(`Failed to get UUID for ${playerName} after ${maxRetries} retries.`);
        return null;
    }

    static delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}