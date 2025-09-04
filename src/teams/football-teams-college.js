// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

import fs from "fs";
import puppeteer from 'puppeteer';
import { Vibrant } from 'node-vibrant/node';
import svg2img from 'svg2img';
import crypto from 'crypto';

import { getBrowserConfigWithHeaders } from '../util/browser-headers.js'
import cacheManager from '../util/cache-manager.js';
import { fetchNcaaTeamData, scrapeNcaaTeamDetails, scrapeHeadCoachFromStatsPage, getNcaaTeamNamesFromIds } from '../util/ncaa-school-util.js';

// Direct configuration values (previously from configs/football-college.yaml)
const ESPN_TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams?limit=200000";
const NCAA_COACH_URLS = [
    "https://en.wikipedia.org/wiki/List_of_current_NCAA_Division_I_FBS_football_coaches",
    "https://en.wikipedia.org/wiki/List_of_current_NCAA_Division_II_football_coaches", 
    "https://en.wikipedia.org/wiki/List_of_current_NCAA_Division_III_football_coaches",
    "https://en.wikipedia.org/wiki/List_of_current_NCAA_Division_I_FCS_football_coaches"
];
const NCAA_ID_URLS = [
    "https://stats.ncaa.org/rankings/national_ranking?academic_year=2025.0&division=11.0&ranking_period=84.0&sport_code=MFB&stat_seq=22.0",
    "https://stats.ncaa.org/rankings/national_ranking?academic_year=2025.0&division=12.0&ranking_period=39.0&sport_code=MFB&stat_seq=27.0",
    "https://stats.ncaa.org/rankings/national_ranking?academic_year=2025.0&division=2.0&ranking_period=34.0&sport_code=MFB&stat_seq=27.0",
    "https://stats.ncaa.org/rankings/national_ranking?academic_year=2025.0&division=3.0&ranking_period=30.0&sport_code=MFB&stat_seq=27.0"
];

const NCAA_TEAM_REF_SELECTOR = "td.reclass a.skipMask[href^='/teams/']";
const NCAA_SHOW_ALL_SELECTOR = "select[name*='length'] option[value='-1'], select[name*='length'] option:last-child";
const COACH_TABLE_SELECTORS = {
    "https://en.wikipedia.org/wiki/List_of_current_NCAA_Division_I_FBS_football_coaches": "(//table)[1]",
    "https://en.wikipedia.org/wiki/List_of_current_NCAA_Division_I_FCS_football_coaches": "(//table)[2]",
    "https://en.wikipedia.org/wiki/List_of_current_NCAA_Division_II_football_coaches": "(//table)[2]",
    "https://en.wikipedia.org/wiki/List_of_current_NCAA_Division_III_football_coaches": "(//table)[1]"
};

const OUTPUT_FILE = "data/processed/football-teams-college.json"
const CHROME_EXEC = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

const NCAA_STAT_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days
const NCAA_COACH_TTL = 365 * 24 * 60 * 60 * 1000; // 1 year
const LOCAL_COLOR_CACHE_TTL = 365 * 24 * 60 * 60 * 1000; // 7 days

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const BROWSER_COLORS = ['\u001b[33m', '\u001b[34m', '\u001b[32m'];

const defaultValue = (value, fallback = null) => (value && value.trim()) ? value.trim() : fallback;

/**
 * Visits ESPN API and extracts football teams
 * 
 * @param {boolean} verbose weather or not to print console messages
 */
const _fetchEspnTeamData = async (verbose) => {
    const espn_response = await fetch(ESPN_TEAMS_URL);
    if (!espn_response.ok) console.log(`\u001b[32mError Downloading ESPN Football Teams: ${ESPN_TEAMS_URL}\u001b[0m`);
    else if (verbose) console.log(`\u001b[32mDownloading ESPN Football Teams: ${ESPN_TEAMS_URL}\u001b[0m`);
    const college_espn_teams = await espn_response.json();
    return college_espn_teams?.sports?.[0]?.leagues?.[0]?.teams.map(x => x.team) || [];
}

/**
 * Fetches or scrapes NCAA team-to-ID bindings. It first checks a cache for fresh data.
 * If the cache is stale or empty, it scrapes the data from stats.ncaa.org, handling
 * pagination to retrieve all teams before caching the results.
 *
 * @param {boolean} verbose - If true, enables detailed logging to the console.
 * @param {Array<import('puppeteer').Browser>} browsers - An array of pre-launched Puppeteer browser instances.
 * @returns {Promise<Array<{ team_name: string, ncaa_id: string }>>} A promise resolving to an array of team-ID binding objects.
 */
const _fetchNcaaIdBindings = async (verbose, browsers) => {
    const cachedResult = cacheManager.get("football_college_ids", NCAA_STAT_TTL);
    if (cachedResult) {
        if (verbose) console.log(`\u001b[36mUsing cached NCAA Football IDs from ${cachedResult.savedAt.toLocaleString()}\u001b[0m`);
        return cachedResult.data;
    }
    const CONCURRENT_BROWSERS = browsers.length;
    // Distribute URLs into batches for each concurrent browser.
    const urlBatches = Array.from({ length: CONCURRENT_BROWSERS }, () => []);
    NCAA_ID_URLS.forEach((link, index) => {
        urlBatches[index % CONCURRENT_BROWSERS].push(link);
    });

    /**
     * Helper to navigate with a retry mechanism.
     * @param {import('puppeteer').Page} page - The Puppeteer page instance.
     * @param {string} url - The URL to navigate to.
     */
    const navigateWithRetry = async (page, url) => {
        try {
            await page.goto(url, { waitUntil: 'networkidle0', timeout: 60000 });
            await sleep(5000);
        } catch (error) {
            if (verbose) console.log(`\u001b[33mNavigation failed for ${url}. Retrying...\u001b[0m`);
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
            await sleep(8000);
        }
    };

    // Map over each batch and process it in a separate browser instance.
    const scrapingPromises = urlBatches.map(async (links, browserIndex) => {
        const browser = browsers[browserIndex];
        const page = await browser.newPage();
        const batchResults = [];
        try {
            const browserConfig = getBrowserConfigWithHeaders({
                'Referer': 'https://stats.ncaa.org/',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-GPC': '1',
                'Upgrade-Insecure-Requests': '1',
                'Priority': 'u=0, i'
            });
            await page.setUserAgent(browserConfig.userAgent);
            await page.setViewport({ width: 1920, height: 1080 });
            await page.setExtraHTTPHeaders(browserConfig.headers);
            for (const link of links) {
                if (verbose) console.log(`${BROWSER_COLORS[browserIndex]}Scraping NCAA IDs: ${link}\u001b[0m`);
                try {
                    await navigateWithRetry(page, link);
                    // Execute scraping logic in the browser context. This function is async
                    // to properly handle waiting for the DOM to update after pagination.
                    const idsOnPage = await page.evaluate(async (showAllSelector, teamRefSelector) => {
                        // Check for a "Show All" dropdown in a paginated table.
                        const showAllButton = document.querySelector(showAllSelector);
                        if (showAllButton) {
                            showAllButton.selected = true;
                            // Dispatch a 'change' event to trigger the table update.
                            showAllButton.parentElement.dispatchEvent(new Event('change', { bubbles: true }));
                            // CRITICAL: Wait for the DOM to update after the event.
                            await new Promise(resolve => setTimeout(resolve, 2000));
                        }
                        // Select all team links and extract the required data.
                        const linkElements = document.querySelectorAll(teamRefSelector);
                        return Array.from(linkElements).map(el => {
                            // Clean the team name by removing parenthetical suffixes (e.g., "(FBS)").
                            const teamName = el.textContent.trim().replace(/\s*\([^)]*\)$/, '');
                            // Extract the numeric ID from the href attribute.
                            const ncaaId = el.getAttribute('href')?.replace('/teams/', '') ?? null;
                            return (teamName && ncaaId) ? { team_name: teamName, ncaa_id: ncaaId } : null;
                        }).filter(Boolean); // Filter out any null results.
                    }, NCAA_SHOW_ALL_SELECTOR, NCAA_TEAM_REF_SELECTOR);
                    batchResults.push(...idsOnPage);
                    await sleep(1000 + Math.random() * 2000);
                } catch (error) {
                    console.error(`${BROWSER_COLORS[browserIndex]}Failed to process ${link}: ${error.message}\u001b[0m`);
                }
            }
        } finally { return batchResults; }
    });
    const scrapedIds = (await Promise.all(scrapingPromises)).flat();
    if (scrapedIds.length > 0) cacheManager.set("football_college_ids", scrapedIds);
    return scrapedIds;
};

/**
 * Matches NCAA teams with their corresponding NCAA IDs using name-based matching.
 * Returns teams that successfully matched with IDs and those that couldn't be matched.
 *
 * @param {Array<Object>} ncaaTeams - Array of NCAA team objects with school details
 * @param {Array<Object>} ncaaIds - Array of NCAA ID bindings for team matching  
 * @param {boolean} verbose - If true, enables detailed logging
 * @returns {{matchedTeams: Array<Object>, unmatchedTeams: Array<Object>}} Object containing matched and unmatched NCAA teams
 */
const _addNcaaIdsToTeams = (ncaaTeams, ncaaIds, verbose) => {
    const teamsWithIds = [];
    const teamsWithoutIds = [];
    ncaaTeams.forEach(team => {
        // Try to find matching NCAA ID by team name (exact match like backup file)
        const matchingId = ncaaIds.find(idRecord => {
            if (!idRecord.team_name || !team.school_name) return false;
            return idRecord.team_name.toLowerCase().trim() === team.school_name.toLowerCase().trim();
        });
        if (matchingId) teamsWithIds.push({ ...team, ncaa_id: matchingId.ncaa_id });
        else teamsWithoutIds.push({ ...team, ncaa_id: null });

    });
    return {
        matchedTeams: teamsWithIds,
        unmatchedTeams: teamsWithoutIds
    };
};

/**
 * Matches ESPN teams with NCAA teams (that have IDs) using binding model and formats the final dataset.
 * Returns matched teams in the final format plus unmatched teams from both sources.
 *
 * @param {Array<Object>} espnTeams - Array of ESPN team objects 
 * @param {Array<Object>} ncaaTeamsWithIds - Array of NCAA team objects that have NCAA IDs
 * @param {boolean} verbose - If true, enables detailed logging
 * @returns {Promise<{matchedTeams: Array<Object>, unmatchedEspn: Array<Object>, unmatchedNcaa: Array<Object>}>} Object containing matched and unmatched teams
 */
const _matchEspnToNcaaTeams = async (espnTeams, ncaaTeamsWithIds, verbose) => {

    // Generate unique ID for each team
    const generateId = (espnId, abbreviation) => {
        const base = abbreviation || espnId || 'unknown';
        return crypto.createHash('sha256').update(`cfb-${base}-${espnId}`).digest('hex').substring(0, 16);
    };
    // Load the simplified binding model for ESPN-NCAA matching
    let bindingModel = {};
    try {
        bindingModel = JSON.parse(fs.readFileSync("data/models/football-espn-ncaa-binding.json", "utf8"));
    } catch (error) {
        if (verbose) console.log(`\u001b[33mWarning: Could not load binding model, using empty bindings: ${error.message}\u001b[0m`);
    }
    // Apply simplified bindings to match ESPN and NCAA data (only NCAA teams with IDs)
    const boundEspnIds = Object.values(bindingModel);
    const boundNcaaIds = Object.keys(bindingModel);
    const combinedEspn = espnTeams.filter(team => boundEspnIds.includes(String(team.id)));
    const combinedNcaa = ncaaTeamsWithIds.filter(team => boundNcaaIds.includes(String(team.ncaa_id)));

    // Create matched teams using simplified bindings
    const matchedData = [];
    Object.entries(bindingModel).forEach(([ncaaId, espnId]) => {
        const espnTeam = combinedEspn.find(team => String(team.id) === espnId);
        const ncaaTeam = combinedNcaa.find(team => String(team.ncaa_id) === ncaaId);
        if (espnTeam && ncaaTeam) {
            matchedData.push({ ...espnTeam, ...ncaaTeam });
        }
    });
    if (verbose) console.log(`\u001b[32mPerforming ESPN-NCAA team matching using hardcoded bindings...\u001b[0m`);
    const matchData = matchedData.map(team => {
        return {
            ncaa_school: team.name_ncaa,
            espn_school: team.location
        }
    });
    // Format matched teams into final structure (with ESPN colors, coaches/venue filled later)
    const formattedTeams = matchedData.map(team => {
        return {
            id: generateId(team.id, team.abbreviation),
            espn_id: team.id,
            ncaa_id: team.ncaa_id,
            type: "NCAAF",
            slug: team.slug || null,
            abv: team.abbreviation || null,
            full_name: team.displayName || null,
            short_name: team.name || null,
            university: team.name_ncaa || team.location || null,
            division: team.division || null,
            conference: team.conference || null,
            primary: team.color ? `#${team.color}` : null,
            secondary: team.alternateColor ? `#${team.alternateColor}` : null,
            logo: team.img_src || null,
            head_coach: null, // Will be filled by coach scraping later
            offensive_coordinator: null, // Will be filled by coordinator matching later
            defensive_coordinator: null, // Will be filled by coordinator matching later
            school_url: team.school_url || null,
            website: team.website || null,
            twitter: team.twitter || null
        };
    });
    // Identify unmatched teams
    const unmatchedEspn = espnTeams.filter(team => !boundEspnIds.includes(String(team.id)));
    const unmatchedNcaa = ncaaTeamsWithIds.filter(team => !boundNcaaIds.includes(String(team.ncaa_id)));
    fs.writeFileSync("output/temp-bindings.json", JSON.stringify(matchData, null, 2), "utf8");
    return {
        matchedTeams: formattedTeams,
        unmatchedEspn: unmatchedEspn,
        unmatchedNcaa: unmatchedNcaa
    };
};

/**
 * Scrapes head coach information from NCAA team stats pages with robust error handling
 * 
 * @param {Array<Object>} teams - Array of team objects with ncaa_id property
 * @param {boolean} verbose - If true, enables detailed logging to the console
 * @param {Array<import('puppeteer').Browser>} browsers - Array of pre-launched browser instances
 * @returns {Promise<Object>} Promise resolving to cache object with ncaa_id as keys and coach names as values
 */
const _scrapeFootballHeadCoaches = async (teams, verbose, browsers) => {
    const CONCURRENT_BROWSERS = browsers.length;
    const cached = cacheManager.get("football_college_coaches", NCAA_COACH_TTL);
    let teamCoachesCache = cached?.data || {};
    const teamsNeedingCoaches = teams.filter(t => t.ncaa_id && !teamCoachesCache[t.ncaa_id]);

    if (teamsNeedingCoaches.length === 0) {
        if (verbose && cached) console.log(`\u001b[36mUsing cached Head Coach data for all teams (from ${cached.savedAt.toLocaleString()}).\u001b[0m`);
        return teamCoachesCache;
    } else if (verbose) {
        const cachedCount = Object.keys(teamCoachesCache).length;
        const cacheMsg = cachedCount ?
            `Found ${cachedCount} cached coaches (from ${cached.savedAt?.toLocaleString()}).` :
            `No cached coaches.`;
        console.log(`\u001b[36m${cacheMsg} Scraping head coaches for ${teamsNeedingCoaches.length} new teams.\u001b[0m`);
    }

    const COACH_BATCH_SIZE = Math.ceil(teamsNeedingCoaches.length / CONCURRENT_BROWSERS);
    const processCoachBatch = async (batch, browser, idx) => {
        const page = await browser.newPage();
        const cfg = getBrowserConfigWithHeaders({
            'Referer': 'https://stats.ncaa.org/',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'Upgrade-Insecure-Requests': '1',
            'Priority': 'u=0, i'
        });

        await page.setUserAgent(cfg.userAgent);
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setExtraHTTPHeaders(cfg.headers);

        try {
            if (verbose) console.log(`${BROWSER_COLORS[idx]}Establishing NCAA session...\u001b[0m`);
            await page.goto('https://stats.ncaa.org/', { waitUntil: 'networkidle0', timeout: 30000 });
            await sleep(3000);
        } catch {
            if (verbose) console.log(`${BROWSER_COLORS[idx]}Warning: Session setup failed, continuing...\u001b[0m`);
        }

        for (const team of batch) {
            if (verbose) console.log(`${BROWSER_COLORS[idx]}Downloading Head Coach: https://stats.ncaa.org/teams/${team.ncaa_id}\u001b[0m`);
            const headCoach = await scrapeHeadCoachFromStatsPage(team.ncaa_id, page, verbose);
            if (team.ncaa_id && headCoach) {
                teamCoachesCache[team.ncaa_id] = defaultValue(headCoach);
                cacheManager.set("football_college_coaches", teamCoachesCache);
            }
            await sleep(5000 + Math.random() * 1000);
        }

        await page.close();
    };
    const batches = [];
    for (let i = 0; i < teamsNeedingCoaches.length; i += COACH_BATCH_SIZE) batches.push(teamsNeedingCoaches.slice(i, i + COACH_BATCH_SIZE));
    await Promise.all(batches.map((batch, i) => processCoachBatch(batch, browsers[i % CONCURRENT_BROWSERS], i)));
    return teamCoachesCache;
};

/**
 * Scrapes coordinator information from Wikipedia pages and caches the results
 * 
 * @param {boolean} verbose - Enable console logging
 * @param {Array<import('puppeteer').Browser>} browsers - Array of pre-launched browser instances
 * @returns {Promise<Array<Object>>}
 */
const _scrapeFootballCoordinators = async (verbose, browsers) => {
    const cacheKey = "football_college_coordinators";
    const cached = cacheManager.get(cacheKey, NCAA_COACH_TTL);
    let coordinatorCache = cached?.data || [];
    if (coordinatorCache.length) {
        if (verbose) console.log(`\u001b[36mUsing cached coordinator data (${coordinatorCache.length} records from ${cached.savedAt.toLocaleString()})\u001b[0m`);
        return coordinatorCache;
    }
    const extractCoordinators = (xpath) => {
        const getNode = (xp) => document.evaluate(xp, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
        const table = getNode(xpath);
        if (!table) return [];
        const rows = [...table.querySelectorAll("tr")];
        let headerRow;
        for (const row of rows) {
            const texts = [...row.querySelectorAll("th, td")].map(c => c.textContent.trim().toLowerCase());
            if (texts.some(t => /team|school/.test(t)) && texts.some(t => /offensive/.test(t)) && texts.some(t => /defensive/.test(t))) {
                headerRow = {
                    team: texts.findIndex(t => /team|school/.test(t)),
                    head: texts.findIndex(t => /head coach/.test(t)),
                    off: texts.findIndex(t => /offensive/.test(t)),
                    def: texts.findIndex(t => /defensive/.test(t))
                };
                break;
            }
        }
        if (!headerRow) return [];
        return rows.slice(1).map(r => {
            const cells = [...r.querySelectorAll("td")];
            if (cells.length <= Math.max(headerRow.team, headerRow.off, headerRow.def)) return null;
            const clean = (val) => val?.replace(/\[[0-9]+\]/g, "").trim() || null;
            return {
                team: clean(cells[headerRow.team]?.textContent),
                head_coach: clean(cells[headerRow.head]?.textContent),
                offensive_coordinator: clean(cells[headerRow.off]?.textContent),
                defensive_coordinator: clean(cells[headerRow.def]?.textContent),
            };
        }).filter(Boolean);
    };
    const scrapeCoordinatorsFromWiki = async (url, page) => {
        try {
            await page.goto(url, { waitUntil: "networkidle0", timeout: 60000 });
            await sleep(3000);
            const tableSelector = COACH_TABLE_SELECTORS[url];
            if (!tableSelector) return [];
            return page.evaluate(extractCoordinators, tableSelector);
        } catch (e) {
            console.error(`\u001b[31mFailed to scrape coordinators from ${url}: ${e.message}\u001b[0m`);
            return [];
        }
    };
    const batches = Array.from({ length: browsers.length }, () => []);
    NCAA_COACH_URLS.forEach((url, i) => batches[i % browsers.length].push(url));
    const allCoordinators = (
        await Promise.all(batches.map(async (urls, i) => {
            const page = await browsers[i].newPage();
            const browserConfig = getBrowserConfigWithHeaders({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://en.wikipedia.org/",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
            });
            await page.setUserAgent(browserConfig.userAgent);
            await page.setViewport({ width: 1920, height: 1080 });
            await page.setExtraHTTPHeaders(browserConfig.headers);
            const results = [];
            for (const url of urls) {
                if (verbose) console.log(`${BROWSER_COLORS[i]}Downloading NCAA Coordinators: ${url}\u001b[0m`);
                results.push(...await scrapeCoordinatorsFromWiki(url, page));
                await sleep(2000);
            }
            await page.close();
            return results;
        }))
    ).flat();
    cacheManager.set(cacheKey, allCoordinators);
    return allCoordinators;
};

/**
 * Matches coordinator data with team records using multiple name matching strategies
 * 
 * @param {string} teamName - The team name to match against coordinator records
 * @param {Array<Object>} coordinatorData - Array of coordinator records from Wikipedia tables
 * @returns {Object|null} Matched coordinator record with offensive/defensive coordinators, or null if no match found
 */
const _matchCoordinatorByName = (teamName, coordinatorData) => {
    if (!teamName) return null;
    // Try exact match first
    let match = coordinatorData.find(coord => coord.team === teamName);
    if (match) return match;
    // Try partial matching - look for team name in coordinator team name
    const teamWords = teamName.toLowerCase().split(/\s+/);
    match = coordinatorData.find(coord => {
        const coordTeam = coord.team.toLowerCase();
        return teamWords.some(word => word.length > 3 && coordTeam.includes(word));
    });
    if (match) return match;
    // Try matching by university name if available
    const universityWords = (teamName || '').toLowerCase().split(/\s+/);
    match = coordinatorData.find(coord => {
        const coordTeam = coord.team.toLowerCase();
        return universityWords.some(word => word.length > 4 && coordTeam.includes(word));
    });
    return match;
};

/**
 * Extracts dominant colors from an image URL using SVG to PNG conversion and color analysis
 * 
 * @param {string} imageUrl - URL of the image to process
 * @param {Array<Object>} colorBindings - Cache array of previously processed images
 * @param {boolean} verbose - If true, enables detailed logging to the console
 * @returns {Promise<string|null>} Comma-separated hex colors or null if extraction fails
 */
const _extractDominantColors = async (imageUrl, colorBindings, verbose) => {
    if (!imageUrl) return null;
    // Check cache first
    const cached = colorBindings.find(binding => binding.url === imageUrl);
    if (cached && cached.colors) return cached.colors;
    // Extract colors from SVG by converting to PNG first
    try {
        if (verbose) console.log(`\u001b[32mDownloading Logo Colors: ${imageUrl}\u001b[0m`);
        // Download the SVG
        const svgResponse = await fetch(imageUrl);
        if (!svgResponse.ok) throw new Error(`HTTP ${svgResponse.status}`);
        const svgBuffer = await svgResponse.arrayBuffer();
        const svgString = new TextDecoder().decode(svgBuffer);
        // Convert SVG to PNG using svg2img
        const pngBuffer = await new Promise((resolve, reject) => {
            svg2img(svgString, { width: 200, height: 200, format: 'png' }, (error, buffer) => {
                if (error) reject(error);
                else resolve(buffer);
            });
        });
        // Use node-vibrant on the converted PNG
        const palette = await Vibrant.from(pngBuffer).getPalette();
        const colors = [];
        // Get the most vibrant colors, prioritizing vibrant over muted
        if (palette.Vibrant) colors.push(palette.Vibrant.hex);
        else if (palette.DarkVibrant) colors.push(palette.DarkVibrant.hex);
        else if (palette.LightVibrant) colors.push(palette.LightVibrant.hex);
        // Get a secondary color
        if (palette.DarkVibrant && colors[0] !== palette.DarkVibrant.hex) {
            colors.push(palette.DarkVibrant.hex);
        } else if (palette.LightVibrant && colors[0] !== palette.LightVibrant.hex) {
            colors.push(palette.LightVibrant.hex);
        } else if (palette.Muted && colors[0] !== palette.Muted.hex) {
            colors.push(palette.Muted.hex);
        }
        if (colors.length > 0) {
            const result = colors.join(', ');
            // Update local cache with new colors
            colorBindings.push({ url: imageUrl, colors: result });
            return result;
        }
        return null;
    } catch (error) {
        if (verbose) console.log(`\u001b[33mWarning: Could not extract colors from ${imageUrl}: ${error.message}\u001b[0m`);
        return null;
    }
};

/**
 * Processes colors to ensure proper hex format
 * 
 * @param {string} color - Color string to process
 * @returns {string|null} Properly formatted hex color with # prefix, or null
 */
const _formatColor = (color) => {
    if (!color || color === 'null' || color === '000000') return null;
    const upperColor = color.toUpperCase();
    return upperColor.startsWith('#') ? upperColor : `#${upperColor}`;
};

/**
 * College Football Teams
 *
 * Retrieves college football team data from ESPN's API and supplements
 * it with additional information scraped from NCAA and CollegeFootballlDB. The combined
 * data is processed into a structured dataframe and saved to a JSON file.
 *
 * @source https://site.api.espn.com/
 * @source https://www.ncaa.com/stats/football/
 * @source https://en.wikipedia.org/wiki/
 *
 * @param {boolean} verbose - Whether to print progress messages (default: true)
 * @param {boolean} save - Whether to save data to data/processed folder
 * 
 * @returns {Array} Array containing the following information for each football team:
 *  * id [string] - A generated unique identifier for each team
 *  * espn_id [number] - id used by espn to identify team
 *  * ncaa_id [string] - id used by ncaa to identify team
 *  * type [string] - Always set to CFB for team type
 *  * slug [string] - Slug used to identify teams
 *  * abv [string] - Abbreviation of team name (ex. TOW)
 *  * full_name [string] - Full name of team (ex. Towson Tigers)
 *  * short_name [string] - Short name of team (ex. Tigers)
 *  * university [string] - University team is located (ex. Towson)
 *  * division [string] - Division team is associated with (ex. I)
 *  * conference [string] - Conference team is associated with (ex. Big West)
 *  * primary [string] - Primary color of team uniforms in Hex format
 *  * secondary [string] - Secondary color of team uniforms in Hex format
 *  * logo [string] - Link to logo image from ESPN
 *  * head_coach [string] - Current head coach of team
 *  * offensive_coordinator [string] - Current offensive coordinator of team
 *  * defensive_coordinator [string] - Current defensive coordinator of team
 *  * school_url [string] - NCAA url for team
 *  * website [string] - Website url for teams school
 *  * twitter [string] - Twitter handle of team starting with '@'
 *  * venue [string] - Current venue where team plays
 */
async function get_formated_teams(verbose = true, save = true) {

    // Fetch ESPN team data using helper function
    const espn_teams = await _fetchEspnTeamData(verbose);

    // Set up browsers for concurrent scraping
    const CONCURRENT_BROWSERS = 3;
    const browsers = [];
    for (let i = 0; i < CONCURRENT_BROWSERS; i++) {
        browsers.push(await puppeteer.launch({
            headless: true,
            executablePath: CHROME_EXEC,
            args: ['--no-sandbox', '--disable-setuid-sandbox']
        }));
    }

    // Fetch NCAA team data
    const ncaa_teams = await fetchNcaaTeamData(verbose, browsers);
    // Fetch NCAA ID bindings
    const ncaa_ids = await _fetchNcaaIdBindings(verbose, browsers);
    // Scrape NCAA team details
    const processedNcaaTeams = await scrapeNcaaTeamDetails(ncaa_teams, verbose, browsers);
    // Match NCAA teams with IDs
    const ncaaIdMatching = _addNcaaIdsToTeams(processedNcaaTeams, ncaa_ids, verbose);
    const { matchedTeams: ncaaTeamsWithIds, unmatchedTeams: ncaaTeamsWithoutIds } = ncaaIdMatching;
    // Save NCAA teams without assigned ids (colleges with no accounted for football team)
    const ncaa_id_output_file = "output/unmatched-ncaa-football-ids.json"
    if (save) fs.writeFileSync(ncaa_id_output_file, JSON.stringify(ncaaTeamsWithoutIds, null, 2), "utf8");
    if (verbose && save) console.log(`\u001b[90mCollege Unmatched Ncaa Id Data Saved To: ${ncaa_id_output_file}\u001b[0m`);

    // Match ESPN teams with NCAA teams and format final dataset
    const teamMatching = await _matchEspnToNcaaTeams(espn_teams, ncaaTeamsWithIds, verbose);
    const { matchedTeams: finalTeams, unmatchedEspn, unmatchedNcaa } = teamMatching;

    // Save unmatched teams to file (does not unclude ncaa teams with no id binding)
    const espn_output_file = "output/unmatched-espn-football-teams.json"
    if (save) fs.writeFileSync(espn_output_file, JSON.stringify(unmatchedEspn, null, 2), "utf8");
    if (verbose && save) console.log(`\u001b[90mCollege Unmatched Espn Data Saved To: ${espn_output_file}\u001b[0m`);
    const ncaa_output_file = "output/unmatched-ncaa-football-teams.json"
    if (save) fs.writeFileSync(ncaa_output_file, JSON.stringify(unmatchedNcaa, null, 2), "utf8");
    if (verbose && save) console.log(`\u001b[90mCollege Unmatched Ncaa Data Saved To: ${ncaa_output_file}\u001b[0m`);

    // TODO: MAP UNMATCHED ESPN TEAMS TO TEAM OBJECTS WITH NULL NCAA_ID ALL OTHER DATA NEEDS TO BE MAPPED FROM PURE ESPN DATA

    // Scrape head coaches based on NCAA_ID
    const teamCoachesCache = await _scrapeFootballHeadCoaches(ncaaTeamsWithIds, verbose, browsers);
    // Scrape coordinators from Wikipedia
    const coordinatorCache = await _scrapeFootballCoordinators(verbose, browsers);

    // Load color bindings cache and remote data if needed
    const { data: colorBindings = [], savedAt } = cacheManager.get("football_college_colors", LOCAL_COLOR_CACHE_TTL) || {};
    if (verbose && savedAt) console.log(`\u001b[36mUsing local color cache (${colorBindings.length} entries from ${savedAt.toLocaleString()})\u001b[0m`);

    // Close all browser instances with error handling
    await Promise.all(browsers.map(async b => {
        if (!b || b.process()?.killed) return;
        try { await b.close() } catch { b.process()?.kill("SIGKILL") }
    }));

    // Process final teams and add coach data and colors
    const processedFinalTeams = await Promise.all(finalTeams.map(async team => {
        // Try to match coordinator data using different team name variations
        const coordinatorMatch =
            _matchCoordinatorByName(team.full_name, coordinatorCache) ||
            _matchCoordinatorByName(team.short_name, coordinatorCache) ||
            _matchCoordinatorByName(team.university, coordinatorCache) ||
            _matchCoordinatorByName(team.abv, coordinatorCache);
        // Get head coach from NCAA ID cache (if available) - this has precedence over coordinator cache
        const headCoachFromCache = team.ncaa_id ? teamCoachesCache[team.ncaa_id] : null;
        // Get best colors from team logo or defualt to espns asigned colors
        const colors = (await _extractDominantColors(team.logo, colorBindings, verbose))?.split(', ') || [];
        const primaryColor = colors[0] || team.primary;
        const secondaryColor = colors[1] || team.secondary;
        return {
            ...team,
            // Head coach: NCAA cache has precedence, fallback to coordinator cache
            head_coach: headCoachFromCache || coordinatorMatch?.head_coach || null,
            // Coordinators always come from coordinator cache
            offensive_coordinator: coordinatorMatch?.offensive_coordinator || null,
            defensive_coordinator: coordinatorMatch?.defensive_coordinator || null,
            // Use formatted colors with proper fallback logic
            primary: _formatColor(primaryColor),
            secondary: _formatColor(secondaryColor)
        };
    }));

    // Save updated color bindings cache
    if (colorBindings.length > 0) cacheManager.set("football_college_colors", colorBindings);

    // Save data as JSON - more efficient and maintains data types
    if (save) fs.writeFileSync(OUTPUT_FILE, JSON.stringify(processedFinalTeams, null, 2), "utf8");
    if (verbose && save) console.log(`\u001b[90mCollege Football Data Saved To: ${OUTPUT_FILE}\u001b[0m`);
    return processedFinalTeams;

}

/**
 * Research potential new bindings between ESPN and NCAA teams
 * @param {Array<Object>} potentialBindings - Array of potential bindings from games script
 * @param {Array<Object>} browsers - Array of browser instances for scraping
 * @param {boolean} verbose - Whether to log progress
 * @returns {Promise<Object>} Object with results of binding research
 */
export const researchPotentialNewBindings = async (potentialBindings = [], verbose = true, browsers) => {
    const results = { processed: 0, successful: 0, failed: 0, newBindings: {}, errors: [] };
    if (!potentialBindings || potentialBindings.length === 0) return results;
    if (verbose) console.log(`\u001b[36mResearching ${potentialBindings.length} potential ESPN-NCAA bindings...\u001b[0m`);

    // Load current binding model
    const bindingModelPath = "data/models/football-espn-ncaa-binding.json";
    let currentBindings = {};
    try {
        currentBindings = JSON.parse(fs.readFileSync(bindingModelPath, "utf8"));
    } catch (error) { if (verbose) console.log(`\u001b[33mWarning: Could not load binding model: ${error.message}\u001b[0m`); }

    // Load NCAA IDs cache
    const ncaaIdsPath = "data/raw/football_college_ids.json";
    let ncaaIdsCache = [];
    try {
        ncaaIdsCache = JSON.parse(fs.readFileSync(ncaaIdsPath, "utf8"));
    } catch (error) { if (verbose) console.log(`\u001b[33mWarning: Could not load NCAA IDs cache: ${error.message}\u001b[0m`); }

    // If we dont have bindings or ncaa ids return empty results to avoid clearing on error
    if (ncaaIdsCache.length < 1 || Object.keys(currentBindings).length < 1) return results

    // Filter potential binding for only ncaa_ids without an existng binding
    const newNcaaIds = potentialBindings.map(b => b.ncaaId).filter(id => id && !currentBindings[id]);
    // If no new bindings return empty results
    if (newNcaaIds.length === 0) return results;
    // Map the new ids to array and send to the util funtion for processing
    const uniqueNewNcaaIds = [...new Set(newNcaaIds)];
    const teamNamesMap = await getNcaaTeamNamesFromIds(uniqueNewNcaaIds, browsers, verbose);

    for (const ncaaId in teamNamesMap) {
        const teamName = teamNamesMap[ncaaId];
        const binding = potentialBindings.find(b => b.ncaaId === ncaaId);
        if (teamName && binding?.espnId) {
            results.processed++;
            // Add to binding model
            currentBindings[ncaaId] = binding.espnId;
            results.newBindings[ncaaId] = binding.espnId;
            // Check for existing ID or team_name
            const existing = ncaaIdsCache.find(item => item.ncaa_id === ncaaId || item.team_name === teamName);
            if (!existing) {
                currentBindings[ncaaId] = binding.espnId;
                results.newBindings[ncaaId] = binding.espnId;
                ncaaIdsCache.push({ team_name: teamName, ncaa_id: ncaaId });
            }
            else if (existing.ncaa_id !== ncaaId) {
                // Duplicate team_name but with a different NCAA ID - handle consolidation
                const { addDuplicateMapping } = await import('../util/ncaa-id-consolidation.js');
                
                const canonicalNcaaId = existing.ncaa_id; 
                const duplicateNcaaId = ncaaId;
                
                // Add mapping: duplicate -> canonical
                addDuplicateMapping(duplicateNcaaId, canonicalNcaaId);
                
                // Map the ESPN ID to the canonical NCAA ID instead
                const canonicalEspnBinding = currentBindings[canonicalNcaaId];
                if (canonicalEspnBinding) {
                    // Canonical NCAA ID already has an ESPN binding
                    if (canonicalEspnBinding !== binding.espnId) {
                        if (verbose) console.log(`\u001b[33mWarning: ESPN ID conflict for "${teamName}". Canonical NCAA ID ${canonicalNcaaId} -> ESPN ${canonicalEspnBinding}, but duplicate NCAA ID ${duplicateNcaaId} suggests ESPN ${binding.espnId}\u001b[0m`);
                    }
                    // Keep the existing canonical binding
                    results.newBindings[duplicateNcaaId] = canonicalEspnBinding;
                } else {
                    // Canonical NCAA ID doesn't have an ESPN binding yet, use this one
                    currentBindings[canonicalNcaaId] = binding.espnId;
                    results.newBindings[canonicalNcaaId] = binding.espnId;
                    results.newBindings[duplicateNcaaId] = binding.espnId; // For tracking purposes
                }
            }
            results.successful++;
        } else {
            results.failed++;
            results.errors.push(`Failed to create binding for NCAA ID: ${ncaaId}`);
        }
    }
    // update the espn->ncaa bindings table with any new bindings
    fs.writeFileSync(bindingModelPath, JSON.stringify(currentBindings, null, 2), "utf8");
    if (verbose) console.log(`\u001b[32mSuccessfully updated and saved the ESPN-NCAA binding model.\u001b[0m`);
    // update the ncaa team_name -> ncaa_id binding
    fs.writeFileSync(ncaaIdsPath, JSON.stringify(ncaaIdsCache, null, 2), "utf8");
    if (verbose) console.log(`\u001b[32mSuccessfully updated and saved the NCAA IDs cache.\u001b[0m`);
    if (verbose) console.log(`\u001b[36mBinding research complete. Processed: ${results.processed}, Successful: ${results.successful}, Failed: ${results.failed}\u001b[0m`);
    return results;
};

// Export the main function as default
export default get_formated_teams;

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) get_formated_teams();