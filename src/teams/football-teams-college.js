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
import yaml from "js-yaml";
import puppeteer from 'puppeteer';
import { Vibrant } from 'node-vibrant/node';
import svg2img from 'svg2img';
import crypto from 'crypto';

import { getBrowserConfigWithHeaders } from '../util/browser-headers.js'
import cacheManager from '../util/cache-manager.js';

const CONFIG_FILE = "configs/football-college.yaml"
const OUTPUT_FILE = "data/processed/football-teams-college.json"
const CHROME_EXEC = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

const NCAA_STAT_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days
const NCAA_DETAIL_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days
const NCAA_COACH_TTL = 365 * 24 * 60 * 60 * 1000; // 1 year

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const BROWSER_COLORS = ['\u001b[33m', '\u001b[34m', '\u001b[32m'];

const defaultValue = (value, fallback = null) => (value && value.trim()) ? value.trim() : fallback;

/**
 * Visits espn api and extracts teams from a predefined link, the espn link must be avalaible
 * under `CONFIG.LINKS.ESPN_TEAMS` to be succesfully extracted
 * 
 * @param {yaml} config configuration with espn team link
 * @param {boolean} verbose weather or not to print console messages
 */
const _fetchEspnTeamData = async (config, verbose) => {
    const espn_response = await fetch(config.LINKS.ESPN_TEAMS);
     if (!espn_response.ok) console.log(`\u001b[32mError Downloading ESPN Football Teams: ${config.LINKS.ESPN_TEAMS}\u001b[0m`);
     else if (verbose) console.log(`\u001b[32mDownloading ESPN Football Teams: ${config.LINKS.ESPN_TEAMS}\u001b[0m`);
    const college_espn_teams = await espn_response.json();
    return college_espn_teams?.sports?.[0]?.leagues?.[0]?.teams.map(x => x.team) || [];
}

/**
 * Fetches or scrapes NCAA team-to-ID bindings. It first checks a cache for fresh data.
 * If the cache is stale or empty, it scrapes the data from stats.ncaa.org, handling
 * pagination to retrieve all teams before caching the results.
 *
 * @param {object} config - The application's configuration object, containing links and CSS selectors.
 * @param {boolean} verbose - If true, enables detailed logging to the console.
 * @param {Array<import('puppeteer').Browser>} browsers - An array of pre-launched Puppeteer browser instances.
 * @returns {Promise<Array<{ team_name: string, ncaa_id: string }>>} A promise resolving to an array of team-ID binding objects.
 */
const _fetchNcaaIdBindings = async (config, verbose, browsers) => {
    const { LINKS, ATTRIBUTES } = config;
    const cachedResult = cacheManager.get("football_college_ids", NCAA_STAT_TTL);
    if (cachedResult) {
        if (verbose) console.log(`\u001b[36mUsing cached NCAA Football IDs from ${cachedResult.savedAt.toLocaleString()}\u001b[0m`);
        return cachedResult.data;
    }
    const CONCURRENT_BROWSERS = browsers.length;
    // Distribute URLs into batches for each concurrent browser.
    const urlBatches = Array.from({ length: CONCURRENT_BROWSERS }, () => []);
    LINKS.NCAA_IDS.forEach((link, index) => {
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
                    const idsOnPage = await page.evaluate(async (selectors) => {
                        const { NCAA_PAGINATION, NCAA_TEAM_REF } = selectors;
                        // Check for a "Show All" dropdown in a paginated table.
                        const showAllButton = document.querySelector(NCAA_PAGINATION.SHOW_ALL_SELECTOR);
                        if (showAllButton) {
                            showAllButton.selected = true;
                            // Dispatch a 'change' event to trigger the table update.
                            showAllButton.parentElement.dispatchEvent(new Event('change', { bubbles: true }));
                            // CRITICAL: Wait for the DOM to update after the event.
                            await new Promise(resolve => setTimeout(resolve, 2000));
                        }
                        // Select all team links and extract the required data.
                        const linkElements = document.querySelectorAll(NCAA_TEAM_REF);
                        return Array.from(linkElements).map(el => {
                            // Clean the team name by removing parenthetical suffixes (e.g., "(FBS)").
                            const teamName = el.textContent.trim().replace(/\s*\([^)]*\)$/, '');
                            // Extract the numeric ID from the href attribute.
                            const ncaaId = el.getAttribute('href')?.replace('/teams/', '') ?? null;
                            return (teamName && ncaaId) ? { team_name: teamName, ncaa_id: ncaaId } : null;
                        }).filter(Boolean); // Filter out any null results.
                    }, ATTRIBUTES);
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
 * Scrapes school data from multiple pages of the ncaa.com schools index, distributing the workload
 * across multiple concurrent browser instances for efficiency. This function serves as the primary
 * data source for the initial list of schools.
 *
 * @param {object} config - The application's configuration object, containing necessary selectors.
 * @param {boolean} verbose - If true, enables detailed logging to the console.
 * @param {Array<import('puppeteer').Browser>} browsers - An array of pre-launched Puppeteer browser instances to use for scraping.
 * @returns {Promise<Array<{school_name: string, school_url: string, img_src: string | null}>>} A promise that resolves to a flattened array of scraped school objects.
 */
const _fetchNcaaSchoolsList = async (config, verbose, browsers) => {
    const CONCURRENT_BROWSERS = browsers.length;
    const MAX_PAGES = 23; // Total number of pages to scrape in the schools index.
    const BASE_URL = 'https://www.ncaa.com/schools-index';
    // Create page URLs and distribute across browsers
    const pageUrls = Array.from({ length: MAX_PAGES }, (_, i) => `${BASE_URL}/${i + 1}`);
    const urlBatches = Array.from({ length: CONCURRENT_BROWSERS }, () => []);
    pageUrls.forEach((url, index) => urlBatches[index % CONCURRENT_BROWSERS].push(url));

    const batchPromises = urlBatches.map(async (urls, browserIndex) => {
        const batchResults = [];
        for (const url of urls) {
            if (verbose) console.log(`${BROWSER_COLORS[browserIndex]}Downloading NCAA School List: ${url}\u001b[0m`);
            try {
                const response = await fetch(url);
                if (!response.ok) continue;
                const html = await response.text();
                
                const schoolLinkRegex = /<a[^>]+href="\/schools\/([^"]+)"[^>]*>([^<]+)<\/a>/g;
                let match;
                while ((match = schoolLinkRegex.exec(html)) !== null) {
                    const [, slug, name] = match;
                    if (slug && name && name.trim()) {
                        batchResults.push({
                            school_name: name.trim(),
                            school_url: `https://www.ncaa.com/schools/${slug}`,
                            img_src: `https://www.ncaa.com/sites/default/files/images/logos/schools/bgl/${slug}.svg`
                        });
                    }
                }
                await sleep(100);
            } catch (error) {
                if (verbose) console.log(`${BROWSER_COLORS[browserIndex]}Warning: Failed to scrape ${url}: ${error.message}\u001b[0m`);
            }
        }
        return batchResults;
    });
    
    const batchResults = await Promise.all(batchPromises);
    return batchResults.flat();
};

/**
 * Orchestrates the fetching of all NCAA team data. It first attempts to retrieve a fresh list
 * from the cache. If the cache is empty or stale, it concurrently scrapes data from both a
 * primary and a backup source, merges the lists while prioritizing the primary source,
 * caches the final unique list, and then returns it.
 *
 * @param {object} config - The application's configuration object, containing links and selectors.
 * @param {boolean} verbose - If true, enables detailed logging to the console.
 * @param {Array<import('puppeteer').Browser>} browsers - An array of pre-launched Puppeteer browser instances.
 * @returns {Promise<Array<Object>>} A promise that resolves to a comprehensive, deduplicated array of NCAA team objects.
 */
const _fetchNcaaTeamData = async (config, verbose, browsers) => {
    const CACHE_KEY = "ncaa_schools_backup";
    const cachedResult = cacheManager.get(CACHE_KEY, NCAA_STAT_TTL);
    if (cachedResult) {
        if (verbose) console.log(`\u001b[36mUsing cached NCAA Football Teams data from ${cachedResult.savedAt.toLocaleString()}\u001b[0m`)
        // If valid cache exists, return it immediately.
        return cachedResult.data;
    }    
    // Fetch initial list of schools (name, URL, img_src)
    const schoolsList = await _fetchNcaaSchoolsList(config, verbose, browsers);
    if (schoolsList.length > 0) cacheManager.set(CACHE_KEY, schoolsList);
    return schoolsList;
};

/**
 * Scrapes detailed NCAA team information from school pages and updates team data with divisions
 * where missing. It uses a unified cache keyed by URL and handles concurrent processing.
 *
 * @param {Array<Object>} ncaaTeams - Array of NCAA team objects to process
 * @param {object} config - The application's configuration object
 * @param {boolean} verbose - If true, enables detailed logging
 * @param {Array<import('puppeteer').Browser>} browsers - Pre-launched browser instances
 * @returns {Promise<Array<Object>>} Promise resolving to teams with additional detail fields
 */
const _scrapeNcaaTeamDetails = async (ncaaTeams, config, verbose, browsers) => {
    const CONCURRENT_BROWSERS = browsers.length;

    /**
     * Generates a consistent cache key from NCAA school URLs by normalizing different URL formats.
     * This ensures that equivalent URLs from different NCAA domains map to the same cache entry.
     * 
     * @param {string|null|undefined} schoolUrl - The full NCAA school URL to convert to a cache key
     * @returns {string|null} Normalized URL path for use as cache key, or null if input is invalid
     */
    const getUrlKey = (schoolUrl) => {
        if (!schoolUrl) return null;
        // Normalize stats.ncaa.org URLs to relative paths
        if (schoolUrl.includes('stats.ncaa.org/schools/')) return schoolUrl.replace('https://stats.ncaa.org', '');
        // Normalize www.ncaa.com URLs to relative paths  
        if (schoolUrl.includes('ncaa.com/schools/')) return schoolUrl.replace('https://www.ncaa.com', '');
        // Return as-is for other URLs (fallback)
        return schoolUrl;
    };

    /**
     * Maps various division text formats to standardized division codes.
     * Handles NCAA division text scraped from web pages and normalizes it to consistent values.
     * 
     * @param {string|null|undefined} divisionText - Raw division text from scraped content
     * @returns {string|null} Standardized division code or null if no match
     * 
     */
    const mapDivisionToStandard = (divisionText) => {
        if (!divisionText) return null;
        const text = divisionText.toLowerCase();
        if (text.includes('fbs')) return 'fbs';
        if (text.includes('fcs')) return 'fcs';
        // Check for specific divisions first (more specific matches first)
        if (text.includes('division iii')) return 'd3';
        if (text.includes('division ii')) return 'd2';
        // Only match "division i" when it's NOT followed by other text (like "ii" or "iii")
        if (text.match(/division\s+i(?!\w)/)) return 'fbs';
        return null;
    };
    
    // Use unified school details cache keyed by URL
    const cachedSchoolDetailsResult = cacheManager.get("ncaa_school_details_backup", NCAA_DETAIL_TTL);
    let unifiedSchoolDetailsCache = (cachedSchoolDetailsResult && cachedSchoolDetailsResult.data) ? cachedSchoolDetailsResult.data : {};
    
    // Identify teams that need details scraped based on school_url
    const teamsToScrape = ncaaTeams.filter(team => {
        if (!team.school_url) return false;
        let urlKey = getUrlKey(team.school_url)
        if (!urlKey) return false;
        // Check if we already have cached data for this URL
        return !unifiedSchoolDetailsCache[urlKey];
    });

    if (teamsToScrape.length > 0) {
        if (Object.keys(unifiedSchoolDetailsCache).length > 0 && cachedSchoolDetailsResult) {
            if (verbose) console.log(`\u001b[36mFound ${Object.keys(unifiedSchoolDetailsCache).length} schools in unified details cache (from ${cachedSchoolDetailsResult.savedAt.toLocaleString()}). Scraping details for ${teamsToScrape.length} new schools.\u001b[0m`);
        } else if (verbose) console.log(`\u001b[36mScraping details for ${teamsToScrape.length} schools.\u001b[0m`);
        
        // Concurrent processing with multiple browser instances
        const BATCH_SIZE = Math.ceil(teamsToScrape.length / CONCURRENT_BROWSERS);

        // Function to process a batch of teams
        const processBatch = async (teams, browserInstance, batchIndex) => {
            const page = await browserInstance.newPage();
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

            const results = [];
            for (const team of teams) {
                if (verbose) console.log(`${BROWSER_COLORS[batchIndex % CONCURRENT_BROWSERS]}Downloading School Details: ${team.school_url}\u001b[0m`);
                const detailedData = await _scrapeTeamDetailsWithPage(team.school_url, page, config);
                const urlKey = getUrlKey(team.school_url);
                // Store all results in unified cache by URL key
                const result = {
                    urlKey: urlKey,
                    school_url: team.school_url,
                    ncaa_id: team.ncaa_id || null,
                    details: {
                        conference: defaultValue(detailedData?.conference),
                        nickname_ncaa: defaultValue(detailedData?.nickname_ncaa),
                        colors: defaultValue(detailedData?.colors),
                        name_ncaa: defaultValue(detailedData?.name_ncaa),
                        website: defaultValue(detailedData?.website),
                        twitter: defaultValue(detailedData?.twitter)
                    },
                    division: detailedData?.division ? mapDivisionToStandard(detailedData.division) : null
                };
                results.push(result);
                // Update cache immediately after each school is processed
                unifiedSchoolDetailsCache[result.urlKey] = {
                    ...result.details,
                    school_url: result.school_url,
                    scraped_at: new Date().toISOString(),
                    division: result.division // Add division as a direct property
                };
                cacheManager.set("ncaa_school_details_backup", unifiedSchoolDetailsCache);
                // Rate limiting between requests within the same browser
                await sleep(500 + Math.random() * 500);
            }
            await page.close();
            return results;
        };

        // Split teams into batches and process concurrently
        const batches = [];
        for (let i = 0; i < teamsToScrape.length; i += BATCH_SIZE) batches.push(teamsToScrape.slice(i, i + BATCH_SIZE)); 
        const batchPromises = batches.map((batch, index) => 
            processBatch(batch, browsers[index % CONCURRENT_BROWSERS], index)
        );    
        const batchResults = await Promise.all(batchPromises);
    } else {
        if (verbose && cachedSchoolDetailsResult) console.log(`\u001b[36mUsing cached school details data for all schools (from ${cachedSchoolDetailsResult.savedAt.toLocaleString()}).\u001b[0m`);
    }

    // Function to get cached details by URL
    const getCachedDetailsByUrl = (schoolUrl) => {
        const urlKey = getUrlKey(schoolUrl);
        return urlKey ? unifiedSchoolDetailsCache[urlKey] || null : null;
    };
    
    // Merge cached details into all NCAA teams using URL-based matching
    const processedNcaaTeams = ncaaTeams.map(team => {
        // Get details from unified cache by URL
        const cachedDetails = getCachedDetailsByUrl(team.school_url);
        // Merge team data with cached details
        const teamData = {
            ...team,
            // Use cached details (only source for these fields)
            conference: cachedDetails?.conference || null,
            nickname_ncaa: cachedDetails?.nickname_ncaa || null,
            colors: cachedDetails?.colors || null,
            name_ncaa: cachedDetails?.name_ncaa || null,
            website: cachedDetails?.website || null,
            twitter: cachedDetails?.twitter || null,
            
        };
        return teamData;
    });
    return processedNcaaTeams;
};

/**
 * Legacy scraper for NCAA team details (keeping existing functionality)
 * 
 * @param {string} teamUrl - The NCAA school URL to scrape (e.g., https://stats.ncaa.org/schools/...)
 * @param {Object} page - Puppeteer page instance to use for scraping
 * @param {Object} config - Configuration object with selectors
 * @returns {Object|null} Object containing team details or null if scraping fails
 */
const _scrapeTeamDetailsWithPage = async (teamUrl, page, config) => {
    if (!teamUrl) return null;
    try {
        await page.goto(teamUrl, { waitUntil: 'networkidle0', timeout: 30000 });
        await sleep(3000); // Wait longer for content to load
        // Extract detailed team data using config selectors
        const teamDetails = await page.evaluate((config) => {
            // Extracts trimmed text by XPath
            const getTextByXPath = (xpath) => {
                try { return document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue?.textContent.trim() || null; }
                catch { return null; }
            };
            // Extracts attribute (or text fallback) by XPath
            const getAttributeByXPath = (xpath) => {
                try { const node = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue; return node?.nodeValue || node?.textContent.trim() || null; }
                catch { return null; }
            };
            // Also extract division information from the page
            let division = null;
            // First, try to extract Division I/II/III
            const divEl = document.querySelector(config.NCAA_DETAILED.DIVISION);
            if (divEl) {
                const match = divEl.textContent.match(/Division\s+[IVX]+/i);
                if (match) division = match[0]; 
            }
            // If Division I is found, try to refine with FCS/FBS
            if (division.toLowerCase().match(/division\s+i(?!\w)/)) {
                const tagLineText = getTextByXPath(config.NCAA_DETAILED.SUB_DIVISION)
                if (tagLineText) {
                    const match = tagLineText.match(/(FCS|FBS) Football/i);
                    if (match && match[1]) division = match[1].toLowerCase();
                }
            }
            return {
                conference: getTextByXPath(config.NCAA_DETAILED.CONFERENCE),
                nickname_ncaa: getTextByXPath(config.NCAA_DETAILED.NICKNAME),
                colors: getTextByXPath(config.NCAA_DETAILED.COLORS),
                name_ncaa: getTextByXPath(config.NCAA_DETAILED.SCHOOL),
                website: getAttributeByXPath(config.NCAA_DETAILED.WEBSITE),
                twitter: getAttributeByXPath(config.NCAA_DETAILED.TWITTER),
                division: division
            };
        }, config.ATTRIBUTES);
        return teamDetails;
    } catch (error) {
        console.error(`Download Failed: ${teamUrl}: `, error);
        return {
            conference: null,
            nickname_ncaa: null,
            colors: null,
            name_ncaa: null,
            website: null,
            twitter: null,
            division: null
        };
    }
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
        if (matchingId) teamsWithIds.push({...team, ncaa_id: matchingId.ncaa_id});
        else teamsWithoutIds.push({...team, ncaa_id: null});
        
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
    // Load the binding model for ESPN-NCAA matching
    let matchingModel;
    try {
        const modelData = fs.readFileSync('data/models/espn-ncaa-binding-model.json', 'utf8');
        matchingModel = JSON.parse(modelData);
    } catch (error) {
        if (verbose) console.log(`\u001b[33mWarning: Could not load binding model, using empty bindings\u001b[0m`);
        matchingModel = { sport_bindings: [], model_threshold: 4.58e-05 };
    }
    // Get football bindings
    const sportBindings = matchingModel.sport_bindings.filter(binding => binding.sport === 'football');
    // Apply hardcoded bindings to match ESPN and NCAA data (only NCAA teams with IDs)
    const boundEspnIds = sportBindings.map(b => b.espn_id);
    const boundNcaaIds = sportBindings.map(b => b.ncaa_id);
    const combinedEspn = espnTeams.filter(team => boundEspnIds.includes(String(team.id)));
    const combinedNcaa = ncaaTeamsWithIds.filter(team => boundNcaaIds.includes(String(team.ncaa_id)));
    // Create matched teams using bindings
    const matchedData = [];
    sportBindings.forEach(binding => {
        const espnTeam = combinedEspn.find(team => String(team.id) === binding.espn_id);
        const ncaaTeam = combinedNcaa.find(team => String(team.ncaa_id) === binding.ncaa_id);
        if (espnTeam && ncaaTeam) {
            matchedData.push({ ...espnTeam, ...ncaaTeam });
        }
    });
    if (verbose) console.log(`\u001b[32mPerforming ESPN-NCAA team matching using hardcoded bindings...\u001b[0m`);
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
            logo: team.logo || null,
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
    return {
        matchedTeams: formattedTeams,
        unmatchedEspn: unmatchedEspn,
        unmatchedNcaa: unmatchedNcaa
    };
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
    
    const CONFIG = yaml.load(fs.readFileSync(CONFIG_FILE, "utf8"))
    
    // Fetch ESPN team data using helper function
    const espn_teams = await _fetchEspnTeamData(CONFIG, verbose);
    
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
    const ncaa_teams = await _fetchNcaaTeamData(CONFIG, verbose, browsers);
    // Fetch NCAA ID bindings
    const ncaa_ids = await _fetchNcaaIdBindings(CONFIG, verbose, browsers);    
    // Scrape NCAA team details
    const processedNcaaTeams = await _scrapeNcaaTeamDetails(ncaa_teams, CONFIG, verbose, browsers);    
    // Match NCAA teams with IDs
    const ncaaIdMatching = _addNcaaIdsToTeams(processedNcaaTeams, ncaa_ids, verbose);
    const { matchedTeams: ncaaTeamsWithIds, unmatchedTeams: ncaaTeamsWithoutIds } = ncaaIdMatching;
    // Match ESPN teams with NCAA teams and format final dataset
    const teamMatching = await _matchEspnToNcaaTeams(espn_teams, ncaaTeamsWithIds, verbose);
    const { matchedTeams: finalTeams, unmatchedEspn, unmatchedNcaa } = teamMatching;

    // TODO: COACH ADDITIONS BASED ON NCAA_ID
    // TODO: ADDITIONAL COACHES BASED ON WIKI DATA
    // TODO: COLOR VERIFICATION
    
    // Close all browser instances
    await Promise.all(browsers.map(browser => browser.close()));
    
    // Return complete pipeline data
    return {
        espn_teams: espn_teams,
        ncaa_teams: processedNcaaTeams,
        ncaa_ids: ncaa_ids,
        ncaa_teams_with_ids: ncaaTeamsWithIds,
        ncaa_teams_without_ids: ncaaTeamsWithoutIds,
        final_teams: finalTeams,
        unmatched_espn: unmatchedEspn,
        unmatched_ncaa: unmatchedNcaa
    };
}

// Export the function as default
export default get_formated_teams;
// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) get_formated_teams();


// TODO: GIVEN A NCAA_ID CAN WE ADD A NEW TEAM TO LOAD (NEED TO UPDATE IDS CACHE, RERUN AND RETURN)