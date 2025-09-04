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

import { getBrowserConfigWithHeaders } from './browser-headers.js';
import cacheManager from './cache-manager.js';

const NCAA_STAT_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days
const NCAA_DETAIL_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const BROWSER_COLORS = ['\u001b[33m', '\u001b[34m', '\u001b[32m'];

const defaultValue = (value, fallback = null) => (value && value.trim()) ? value.trim() : fallback;

/**
 * Scrapes school data from multiple pages of the ncaa.com schools index, distributing the workload
 * across multiple concurrent browser instances for efficiency. This function serves as the primary
 * data source for the initial list of schools.
 *
 * @param {boolean} verbose - If true, enables detailed logging to the console.
 * @param {Array<import('puppeteer').Browser>} browsers - An array of pre-launched Puppeteer browser instances to use for scraping.
 * @returns {Promise<Array<{school_name: string, school_url: string, img_src: string | null}>>} A promise that resolves to a flattened array of scraped school objects.
 */
const _fetchNcaaSchoolsList = async (verbose, browsers) => {
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
 * @param {boolean} verbose - If true, enables detailed logging to the console.
 * @param {Array<import('puppeteer').Browser>} browsers - An array of pre-launched Puppeteer browser instances.
 * @returns {Promise<Array<Object>>} A promise that resolves to a comprehensive, deduplicated array of NCAA team objects.
 */
const fetchNcaaTeamData = async (verbose, browsers) => {
    const CACHE_KEY = "ncaa_schools_backup";
    const cachedResult = cacheManager.get(CACHE_KEY, NCAA_STAT_TTL);
    if (cachedResult) {
        if (verbose) console.log(`\u001b[36mUsing cached NCAA Football Teams data from ${cachedResult.savedAt.toLocaleString()}\u001b[0m`)
        // If valid cache exists, return it immediately.
        return cachedResult.data;
    }    
    // Fetch initial list of schools (name, URL, img_src)
    const schoolsList = await _fetchNcaaSchoolsList(verbose, browsers);
    if (schoolsList.length > 0) cacheManager.set(CACHE_KEY, schoolsList);
    return schoolsList;
};

/**
 * Scrapes detailed NCAA team information from school pages and updates team data with divisions
 * where missing. It uses a unified cache keyed by URL and handles concurrent processing.
 *
 * @param {Array<Object>} ncaaTeams - Array of NCAA team objects to process
 * @param {boolean} verbose - If true, enables detailed logging
 * @param {Array<import('puppeteer').Browser>} browsers - Pre-launched browser instances
 * @returns {Promise<Array<Object>>} Promise resolving to teams with additional detail fields
 */
const scrapeNcaaTeamDetails = async (ncaaTeams, verbose, browsers) => {
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
            if (verbose) console.log(`\u001b[36mFound ${Object.keys(unifiedSchoolDetailsCache).length} schools in details cache (from ${cachedSchoolDetailsResult.savedAt.toLocaleString()}). Scraping details for ${teamsToScrape.length} new schools.\u001b[0m`);
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
                const detailedData = await _scrapeTeamDetailsWithPage(team.school_url, page);
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
        if (verbose && cachedSchoolDetailsResult) console.log(`\u001b[36mUsing cached NCAA school details for all schools (from ${cachedSchoolDetailsResult.savedAt.toLocaleString()}).\u001b[0m`);
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
            division: cachedDetails?.division || null,
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
 * @returns {Object|null} Object containing team details or null if scraping fails
 */
const _scrapeTeamDetailsWithPage = async (teamUrl, page) => {
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
            const divEl = document.querySelector(".division-location");
            if (divEl) {
                const match = divEl.textContent.match(/Division\s+[IVX]+/i);
                if (match) division = match[0]; 
            }
            // If Division I is found, try to refine with FCS/FBS
            if (division && division.toLowerCase().match(/division\s+i(?!\w)/)) {
                const tagLineText = getTextByXPath('//*[@id="item-0"]/p')
                if (tagLineText) {
                    const match = tagLineText.match(/(FCS|FBS) Football/i);
                    if (match && match[1]) division = match[1].toLowerCase();
                }
            }
            return {
                conference: getTextByXPath("//dl[contains(@class, 'school-details')]//dt[text()='Conference']/following-sibling::dd"),
                nickname_ncaa: getTextByXPath("//dl[contains(@class, 'school-details')]//dt[text()='Nickname']/following-sibling::dd"),
                colors: getTextByXPath("//dl[contains(@class, 'school-details')]//dt[text()='Colors']/following-sibling::dd"),
                name_ncaa: getTextByXPath("//h1[contains(@class, 'school-name')]"),
                website: getAttributeByXPath("//div[contains(@class, 'school-links')]//a[not(contains(@href, 'twitter.com')) and not(contains(@href, 'facebook.com')) and not(contains(@href, 'instagram.com'))]/@href"),
                twitter: getAttributeByXPath("//div[contains(@class, 'school-links')]//a[contains(@href, 'twitter.com')]/@href"),
                division: division
            };
        });
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
 * A higher-order function that wraps a scraping action with robust retry and session-handling logic
 * for stats.ncaa.org.
 *
 * @param {import('puppeteer').Page} page - The Puppeteer page instance to use.
 * @param {string} url - The URL to navigate to and scrape.
 * @param {Function} scrapeAction - An async function that performs the actual scraping on the page.
 *   It receives the page as an argument and should return the scraped data.
 * @param {boolean} verbose - If true, enables detailed logging.
 * @returns {Promise<any|null>} The data returned by scrapeAction, or null if all retries fail.
 */
const _scrapeWithRetry = async (page, url, scrapeAction, verbose) => {
    let result = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            await page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });
            await sleep(1000);
            const pageTitle = await page.title();
            const pageText = await page.evaluate(() => document.body.textContent);
            if (pageTitle.includes('Access Denied') || pageText.includes('Access Denied') || pageText.includes("don't have permission")) {
                if (attempt < 3) {
                    if (verbose) console.log(`\u001b[31mAccess denied for ${url}, re-establishing new session... (attempt ${attempt}/3)\u001b[0m`);
                    await sleep(5000);
                    try {
                        await page.goto('https://stats.ncaa.org/', { waitUntil: 'networkidle0', timeout: 30000 });
                        await sleep(3000);
                    } catch (sessionError) {
                        if (verbose) console.log('Warning: Could not re-establish new session, continuing anyway...');
                    }
                    continue; 
                } else {
                    if (verbose) console.log(`\u001b[31mAccess denied for ${url} after 3 attempts\u001b[0m`);
                    break;
                }
            }
            result = await scrapeAction(page);
            break;
        } catch (error) {
            if (verbose) console.error(`Attempt ${attempt} for ${url}: ${error.message}`);
            await sleep(3000 * attempt);
        }
    }
    return result;
};

/**
 * Scrapes head coach information from a single NCAA team stats page.
 *
 * @param {string} ncaaId - The NCAA team ID for constructing the stats URL.
 * @param {import('puppeteer').Page} page - Puppeteer page instance to use for scraping.
 * @param {boolean} verbose - If true, enables detailed logging.
 * @returns {Promise<string|null>} The head coach's name, or null if not found or scraping fails.
 */
const scrapeHeadCoachFromStatsPage = async (ncaaId, page, verbose) => {
    if (!ncaaId) return null;
    const statsUrl = `https://stats.ncaa.org/teams/${ncaaId}`;
    const coachAction = async (p) => {
        return p.evaluate(() => {
            const getTextByXPath = (xpath) => {
                try {
                    const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                    return result.singleNodeValue ? result.singleNodeValue.textContent.trim() : null;
                } catch (error) {
                    return null;
                }
            };
            return getTextByXPath("//div[contains(@class,'card-header') and contains(text(), 'Coach')]/following-sibling::div//a");
        });
    };
    return await _scrapeWithRetry(page, statsUrl, coachAction, verbose);
};

/**
 * Fetches team names from stats.ncaa.org for a given list of NCAA IDs.
 *
 * @param {Array<string>} ncaaIds - A list of NCAA team IDs to look up.
 * @param {Array<import('puppeteer').Browser>} browsers - An array of pre-launched Puppeteer browser instances.
 * @param {boolean} verbose - If true, enables detailed logging to the console.
 * @returns {Promise<Object>} A promise that resolves to an object mapping NCAA IDs to team names.
 */
const getNcaaTeamNamesFromIds = async (ncaaIds, browsers, verbose) => {
    const CONCURRENT_BROWSERS = browsers.length;
    const idBatches = Array.from({ length: CONCURRENT_BROWSERS }, () => []);
    ncaaIds.forEach((id, index) => idBatches[index % CONCURRENT_BROWSERS].push(id));
    const batchPromises = idBatches.map(async (ids, browserIndex) => {
        const results = {};
        const browser = browsers[browserIndex];
        const page = await browser.newPage();
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
        // Establish session proactively
        try {
            if (verbose) console.log(`${BROWSER_COLORS[browserIndex % BROWSER_COLORS.length]}Establishing NCAA session...\u001b[0m`);
            await page.goto('https://stats.ncaa.org/', { waitUntil: 'networkidle0', timeout: 30000 });
            await sleep(3000);
        } catch (sessionError) {
            if (verbose) console.log(`${BROWSER_COLORS[browserIndex % BROWSER_COLORS.length]}Warning: Session setup failed, continuing anyway...\u001b[0m`);
        }
        for (const id of ids) {
            const url = `https://stats.ncaa.org/teams/${id}`;
            if (verbose) console.log(`${BROWSER_COLORS[browserIndex % BROWSER_COLORS.length]}Fetching team name for NCAA ID: ${id} from ${url}\u001b[0m`);
            const teamNameAction = async (p) => {
                return p.evaluate(() => {
                    const el = document.querySelector('a.nav-link.skipMask.dropdown-toggle[data-toggle="collapse"]');
                    return el ? el.textContent.trim().replace(/ Sports$/, '') : null;
                });
            };
            const teamName = await _scrapeWithRetry(page, url, teamNameAction, verbose);
            if (teamName) results[id] = teamName;
            await sleep(5000 + Math.random() * 1000);
        }
        await page.close();
        return results;
    });
    const batchResults = await Promise.all(batchPromises);
    return batchResults.reduce((acc, res) => ({ ...acc, ...res }), {});
};

export { fetchNcaaTeamData, scrapeNcaaTeamDetails, scrapeHeadCoachFromStatsPage, getNcaaTeamNamesFromIds };