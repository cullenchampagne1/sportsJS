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
const getUrlKey = (schoolUrl) => {
    if (!schoolUrl) return null;
    if (schoolUrl.includes('stats.ncaa.org/schools/')) return schoolUrl.replace('https://stats.ncaa.org', '');
    if (schoolUrl.includes('ncaa.com/schools/')) return schoolUrl.replace('https://www.ncaa.com', '');
    return schoolUrl;
};
const defaultValue = (value, fallback = null) => (value && value.trim()) ? value.trim() : fallback;
const mapDivisionToStandard = (divisionText) => {
    if (!divisionText) return null;
    const text = divisionText.toLowerCase();
    if (text.includes('fbs')) return 'FBS';
    if (text.includes('fcs')) return 'FCS';
    // Check for specific divisions first (more specific matches first)
    if (text.includes('division iii')) return 'D3';
    if (text.includes('division ii')) return 'D2';
    if (text.includes('division i')) return 'FBS';
    return null;
};

/**
 * College Football Teams
 *
 * Retrieves college football team data from ESPN's API and supplements
 * it with additional information scraped from NCAA and CollegeFootballlDB. The combined
 * data is processed into a structured dataframe and saved to a CSV file.
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
    if (verbose) console.log(`\u001b[32mDownloading ESPN Football Teams: ${CONFIG.LINKS.ESPN_TEAMS}\u001b[0m`);
    const response = await fetch(CONFIG.LINKS.ESPN_TEAMS);
    const college_espn_teams = await response.json();
    const espn_team_list = college_espn_teams?.sports?.[0]?.leagues?.[0]?.teams.map(x => x.team) || [];
    
    function extractTeam(team) {
        const baseFields = ["id", "uid", "slug", "abbreviation", "displayName", "shortDisplayName", "name", "nickname", "location", "color", "alternateColor", "isActive", "isAllStar"];
        const baseFieldsObj = Object.fromEntries(baseFields.map(f => [f, team[f] ?? null]));
        baseFieldsObj.logo = team.logos?.[0]?.href ?? null;

        const teamLinks = {};
        if (Array.isArray(team.links)) {
            team.links.forEach((link, i) => {
                let key = link.text.replace(/\s+/g, "_");
                if (teamLinks[key]) key = `${key}_${i}`;
                teamLinks[key] = link.href;
            });
        }
        if (baseFieldsObj.id) {
            baseFieldsObj.espn_id = baseFieldsObj.id;
            delete baseFieldsObj.id;
        }
        return { ...baseFieldsObj, ...teamLinks };
    }

    const espn_teams_raw = espn_team_list.map(extractTeam);
    
    const CONCURRENT_BROWSERS = 3;
    const BROWSER_COLORS = ['\u001b[33m', '\u001b[34m', '\u001b[32m'];
    const browsers = [];
    for (let i = 0; i < CONCURRENT_BROWSERS; i++) {
        browsers.push(await puppeteer.launch({
            headless: true,
            executablePath: CHROME_EXEC,
            args: ['--no-sandbox', '--disable-setuid-sandbox']
        }));
    }
    
    const allTeams = [];
    const cachedTeamsResult = cacheManager.get("ncaa_schools_backup", NCAA_STAT_TTL);
    if (cachedTeamsResult) {
        const { data: cachedTeams, savedAt } = cachedTeamsResult;
        if (verbose) console.log(`\u001b[36mUsing cached NCAA Football Teams data from ${savedAt.toLocaleString()}\u001b[0m`);
        allTeams.push(...cachedTeams);
    } else {
        const linkBatches = Array(CONCURRENT_BROWSERS).fill().map(() => []);
        CONFIG.LINKS.NCAA_TEAMS.forEach((link, index) => linkBatches[index % CONCURRENT_BROWSERS].push(link));
    
        const browserPromises = linkBatches.map(async (links, browserIndex) => {
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
            const browserResults = [];
            for (const link of links) {
                if (verbose) console.log(`${BROWSER_COLORS[browserIndex]}Downloading NCAA Football Teams: ${link}\u001b[0m`);
                // Extract division from URL
                const divisionMatch = link.match(/\/football\/([^/]+)/);
                const division = divisionMatch ? divisionMatch[1] : 'Unknown';
                try {
                    await page.goto(link, { waitUntil: 'networkidle0', timeout: 60000 });
                    await sleep(5000);
                } catch (error) {
                    try {
                        await page.goto(link, { waitUntil: 'domcontentloaded', timeout: 30000 });
                        await sleep(8000);
                    } catch (retryError) {
                        console.error(`Download Failed: ${link}:`);
                        continue;
                    }
                }
                // Extract team data using robust fallback selectors
                const teams = await page.evaluate((config) => {
                    // Try primary selector from config first
                    let rows = document.querySelectorAll(config.ROWS);
                    // If primary selector fails, use fallback
                    if (rows.length === 0) { rows = document.querySelectorAll('table tr'); }
                    const teamData = [];
                    rows.forEach((row) => {
                        const columns = row.querySelectorAll(config.COLUMNS);
                        // Use configured column index
                        if (columns.length > config.COLUMN_NUMBER) {
                            const col = columns[config.COLUMN_NUMBER];
                            // Get image using config selector
                            const imgElem = col.querySelector(config.IMG_SRC);
                            const imgSrc = imgElem ? imgElem.src : null;
                            // Get school link using config selector
                            const schoolElem = col.querySelector(config.SCHOOL_URL);
                            let schoolUrl = null;
                            let schoolName = null;
                            if (schoolElem) {
                                schoolUrl = schoolElem.href;
                                schoolName = schoolElem.textContent.trim();
                            } else {
                                // Fallback: try any link in the column
                                const anyLink = col.querySelector('a');
                                if (anyLink && anyLink.textContent.trim().length > 2) {
                                    schoolUrl = anyLink.href;
                                    schoolName = anyLink.textContent.trim();
                                }
                            }
                            // Basic validation - looks like a team name
                            if (schoolName && schoolName.length > 2 && 
                                !schoolName.includes('NCAA') && !schoolName.includes('Team')) {
                                teamData.push({
                                    school_name: schoolName,
                                    img_src: imgSrc,
                                    school_url: schoolUrl
                                });
                            }
                        }
                    });
                    return teamData;
                }, CONFIG.ATTRIBUTES.NCAA_TEAMS);
                // Add division to each team
                teams.forEach(team => { team.division = division; }); 
                browserResults.push(...teams);
            }
            await page.close();
            return browserResults;
        });
        const browserResults = await Promise.all(browserPromises);
        browserResults.forEach(teams => { allTeams.push(...teams);  });
        // Save to cache
        cacheManager.set("ncaa_schools_backup", allTeams);
    }
    // Keep NCAA teams as array
    const ncaa_teams = allTeams;
    const allIds = [];
    // Check cache first
    const cachedIdsResult = cacheManager.get("football_college_ids", NCAA_STAT_TTL);
    if (cachedIdsResult) {
        const { data: cachedIds, savedAt } = cachedIdsResult;
        if (verbose) console.log(`\u001b[36mUsing cached NCAA Football IDs data from ${savedAt.toLocaleString()}\u001b[0m`);
        allIds.push(...cachedIds);
    } else {
        // Split links into batches for each browser
        const idLinkBatches = [];
        for (let i = 0; i < CONCURRENT_BROWSERS; i++) { idLinkBatches.push([]); }
        CONFIG.LINKS.NCAA_IDS.forEach((link, index) => { idLinkBatches[index % CONCURRENT_BROWSERS].push(link); });
    
        const idBrowserPromises = idLinkBatches.map(async (links, browserIndex) => {
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
            const browserResults = [];
            for (const link of links) {
                if (verbose) console.log(`${BROWSER_COLORS[browserIndex]}Downloading NCAA Football Ids: ${link}\u001b[0m`);
                
                try {
                    await page.goto(link, { waitUntil: 'networkidle0', timeout: 60000 });
                    await sleep(5000);
                } catch (error) {
                    try {
                        await page.goto(link, { waitUntil: 'domcontentloaded', timeout: 30000 });
                        await sleep(8000);
                    } catch (retryError) {
                        console.error(`Download Failed: ${link}:`);
                        continue;
                    }
                }
                // Check if DataTables pagination exists and handle it
                const allTeamIds = await page.evaluate((config) => {
                    let allIds = [];
                    // First, try to get all data by showing all entries if pagination exists
                    const showAllButton = document.querySelector(config.NCAA_PAGINATION.SHOW_ALL_SELECTOR);
                    if (showAllButton) {
                        showAllButton.selected = true;
                        showAllButton.parentElement.dispatchEvent(new Event('change'));
                        // Wait a bit for the table to update
                        setTimeout(() => {}, 1000);
                    }
                    // Extract all team data using config selector
                    const links = document.querySelectorAll(config.NCAA_TEAM_REF);
                    links.forEach(link => {
                        const text = link.textContent.trim().replace(/\([^)]*\)$/, ''); 
                        const href = link.getAttribute('href')?.replace('/teams/', '') || null;
                        if (text && href) {
                            allIds.push({
                                team_name: text,
                                ncaa_id: href
                            });
                        }
                    });
                    return allIds;
                }, CONFIG.ATTRIBUTES);
                // Sleep random 1-3 seconds for rate limiting
                await sleep(1000 + Math.random() * 1000);
                browserResults.push(...allTeamIds);
            }
            await page.close();
            return browserResults;
        });
        const idBrowserResults = await Promise.all(idBrowserPromises);
        idBrowserResults.forEach(ids => { allIds.push(...ids); }); 
        // Save to cache
        cacheManager.set("football_college_ids", allIds);
    }
        
    // Keep NCAA IDs as array and clean team names
    const ncaa_ids = allIds.map(item => ({
        ...item,
        team_name: item.team_name ? item.team_name.trim() : item.team_name
    }));
    
    // Create a simple array for teams without doing ID matching yet
    // ID matching will happen AFTER all caching is complete
    const ncaa_teams_with_placeholder_ids = ncaa_teams.map(team => ({
        ...team,
        ncaa_id: null // Will be filled in later using cached ID data
    }));

    /**
     * Scrape additional NCAA schools from the schools index as backup
     * This helps fill gaps where primary sources don't include all NCAA teams
     * Now runs independently of team cache status - only when backup schools aren't cached
     */
    const scrapeNcaaSchoolsBackup = async () => {
        const maxPages = 23;
        if (verbose) console.log(`\u001b[32mScraping backup NCAA schools from ncaa.com/schools-index (${maxPages} pages)...\u001b[0m`);
        
        // Create page URLs and distribute across browsers
        const pageUrls = Array.from({length: maxPages}, (_, i) => `https://www.ncaa.com/schools-index/${i + 1}`);
        const urlBatches = Array(CONCURRENT_BROWSERS).fill().map(() => []);
        pageUrls.forEach((url, index) => urlBatches[index % CONCURRENT_BROWSERS].push(url));

        const batchPromises = urlBatches.map(async (urls, browserIndex) => {
            const browser = browsers[browserIndex];
            const batchResults = [];
            
            for (const url of urls) {
                if (verbose) console.log(`${BROWSER_COLORS[browserIndex]}Scraping Backup Schools: ${url}\u001b[0m`);
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
                                slug: slug.trim(),
                                name: name.trim(),
                                source: 'ncaa_schools_index'
                            });
                        }
                    }
                    await sleep(100); // Rate limiting
                } catch (error) {
                    if (verbose) console.log(`${BROWSER_COLORS[browserIndex]}Warning: Failed to scrape ${url}: ${error.message}\u001b[0m`);
                }
            }
            return batchResults;
        });

        const batchResults = await Promise.all(batchPromises);
        const backupSchools = batchResults.flat();
        // Cache the results
        return backupSchools;
    };
    
    // Get backup schools - check if we already have data first
    let backupNcaaSchools = [];
    if (!cachedTeamsResult || !cachedTeamsResult.data || cachedTeamsResult.data.length === 0) {
        // Only scrape backup schools if we don't have cached team data
        backupNcaaSchools = await scrapeNcaaSchoolsBackup();
    } else {
        // Extract backup schools from existing cached data
        backupNcaaSchools = cachedTeamsResult.data
            .filter(team => team.school_url && team.school_url.includes('ncaa.com/schools/'))
            .map(team => ({
                slug: team.school_url.split('/').pop(),
                name: team.school_name
            }));
        if (verbose) console.log(`\u001b[36mUsing cached backup schools data (${backupNcaaSchools.length} schools)\u001b[0m`);
    }
    // Convert backup schools to match our cached data format exactly
    // Note: We keep the ncaa.com URLs for basic info but will need different logic for scraping
    const backupFormattedForComparison = backupNcaaSchools.map(school => ({
        school_name: school.name,
        img_src: `https://www.ncaa.com/sites/default/files/images/logos/schools/bgl/${school.slug}.svg`,
        school_url: `https://www.ncaa.com/schools/${school.slug}`,
        division: 'unknown'
    }));
    
    // Compare against current teams (from primary division URLs) to find truly new schools
    const existingUrls = new Set(ncaa_teams.map(team => team.school_url).filter(Boolean));
    // Find truly new schools (by exact URL) not in our cached data
    const newSchoolsFromBackup = backupFormattedForComparison.filter(backupSchool => { return !existingUrls.has(backupSchool.school_url); });
    if (verbose && newSchoolsFromBackup.length > 0) {
        console.log(`\u001b[32mDiscovered ${newSchoolsFromBackup.length} NEW schools from NCAA backup scraping:\u001b[0m`);
        const teamsData = cacheManager.get("ncaa_schools_backup", NCAA_DETAIL_TTL);
        const currentTeams = teamsData ? teamsData.data : [];
        const updatedTeams = [...currentTeams, ...newSchoolsFromBackup];
        cacheManager.set("ncaa_schools_backup", updatedTeams);
    }
    
    // Convert new schools to our NCAA teams format
    const backupAsNcaaTeams = newSchoolsFromBackup.map(school => ({
        school_name: school.school_name,
        name_ncaa: school.school_name,
        conference_name: 'Unknown',
        logo: school.img_src,
        ncaa_id: null, // No ID available from this source
        school_url: school.school_url,
        source: 'ncaa_backup_new'
    }));

    
    // For new backup schools, we'll add them to the existing teams list 
    // and let the existing scraping workflow handle getting division info
    if (verbose && newSchoolsFromBackup.length > 0) {
        console.log(`\u001b[32mAdding ${newSchoolsFromBackup.length} new schools to be processed by existing scraping workflow\u001b[0m`);
    }
    
    // Simply use the backup schools as-is for now (division will be filled by existing scraper)
    const enhancedNewSchools = backupAsNcaaTeams;
    // Work directly with arrays - no DataFrame conversion needed  
    const espnTeams = espn_teams_raw;
    const ncaaTeams = [...ncaa_teams_with_placeholder_ids, ...enhancedNewSchools];
    // For detailed scraping, work with NCAA teams array
    const combinedForScraping = [...ncaaTeams];

    
    /**
     * Legacy scraper for NCAA team details (keeping existing functionality)
     * 
     * @param {string} teamUrl - The NCAA school URL to scrape (e.g., https://stats.ncaa.org/schools/...)
     * @param {Object} page - Puppeteer page instance to use for scraping
     * @returns {Object|null} Object containing team details or null if scraping fails
     *   @returns {string|null} returns.conference - Team's conference name
     *   @returns {string|null} returns.nickname_ncaa - Team's nickname from NCAA
     *   @returns {string|null} returns.colors - Team's official colors
     *   @returns {string|null} returns.name_ncaa - Team's official name from NCAA
     *   @returns {string|null} returns.website - Team's official website URL
     *   @returns {string|null} returns.twitter - Team's Twitter handle URL
     */
    const scrapeTeamDetailsWithPage = async (teamUrl, page) => {
        if (!teamUrl) return null;
        try {
            await page.goto(teamUrl, { waitUntil: 'networkidle0', timeout: 30000 });
            await sleep(3000); // Wait longer for content to load
            // Extract detailed team data using config selectors
            const teamDetails = await page.evaluate((config) => {
                const getTextByXPath = (xpath) => {
                    try {
                        const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                        return result.singleNodeValue ? result.singleNodeValue.textContent.trim() : null;
                    } catch (error) {
                        return null;
                    }
                };
                const getAttributeByXPath = (xpath) => {
                    try {
                        const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                        return result.singleNodeValue ? result.singleNodeValue.nodeValue || result.singleNodeValue.textContent.trim() : null;
                    } catch (error) {
                        return null;
                    }
                };
                // Also extract division information from the page
                let division = null;
                const divisionSelector = config.NCAA_DETAILED.DIVISION;

                if (divisionSelector) {
                    const divisionElement = document.querySelector(divisionSelector);
                    if (divisionElement) {
                        const divisionText = divisionElement.textContent;
                        const divisionMatch = divisionText.match(/(Division\s+[IVX]+)/i);
                        if (divisionMatch) {
                            division = divisionMatch[1];
                        }
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
            }, CONFIG.ATTRIBUTES);
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
     * Scrapes head coach information from NCAA team stats pages
     * 
     * @param {string} ncaaId - The NCAA team ID for constructing the stats URL
     * @param {Object} page - Puppeteer page instance to use for scraping
     * @returns {string|null} The head coach's name, or null if not found or scraping fails
     * 
     * @description This function navigates to https://stats.ncaa.org/teams/{ncaaId} and attempts
     * to extract the head coach name using configured XPath selectors. It includes retry logic
     * for handling access denied errors and session establishment.
     */
    const scrapeHeadCoachWithPage = async (ncaaId, page) => {
        if (!ncaaId) return null;
        const statsUrl = `https://stats.ncaa.org/teams/${ncaaId}`;
        // Retry logic for access denied issues
        for (let attempt = 1; attempt <= 3; attempt++) {
            try {
                await page.goto(statsUrl, { waitUntil: 'networkidle0', timeout: 30000 });
                await sleep(5000);
                
                // Check if we got an access denied page
                const pageTitle = await page.title();
                const pageText = await page.evaluate(() => document.body.textContent);
                
                if (pageTitle.includes('Access Denied') || pageText.includes('Access Denied') || pageText.includes("don't have permission")) {
                    if (attempt < 3) {
                        console.log(`\u001b[31mAccess denied for ${statsUrl}, establishing new session... (attempt ${attempt}/3)\u001b[0m`);
                        await sleep(10000); // Wait 10 seconds
                        // Establish new session by visiting main page
                        try {
                            await page.goto('https://stats.ncaa.org/', { waitUntil: 'networkidle0', timeout: 30000 });
                            await sleep(3000);
                        } catch (sessionError) {
                            console.log('Warning: Could not establish new session, continuing anyway...');
                        }
                        continue;
                    } else {
                        console.log(`\u001b[31mAccess denied for ${statsUrl} after 3 attempts\u001b[0m`);
                        return null;
                    }
                }
                // Extract head coach using config selector
                const headCoach = await page.evaluate((config) => {
                    const getTextByXPath = (xpath) => {
                        try {
                            const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                            return result.singleNodeValue ? result.singleNodeValue.textContent.trim() : null;
                        } catch (error) {
                            return null;
                        }
                    };
                    return getTextByXPath(config.NCAA_STAT.HEAD_COACH);
                }, CONFIG.ATTRIBUTES);
                
                // Success - return the result
                return headCoach;
            } catch (error) {
                if (attempt < 3) {
                    console.log(`Error on attempt ${attempt} for ${statsUrl}, retrying...`);
                    await sleep(5000 * attempt);
                    continue;
                } else {
                    console.error(`Download Failed after 3 attempts: ${statsUrl}: `, error);
                    return null;
                }
            }
        }
        return null;
    };
    
    // Work directly with the array - no DataFrame conversion needed
    const finalTeams = combinedForScraping;
    
    // Use unified school details cache keyed by URL
    const cachedSchoolDetailsResult = cacheManager.get("ncaa_school_details_backup", NCAA_DETAIL_TTL);
    let unifiedSchoolDetailsCache = (cachedSchoolDetailsResult && cachedSchoolDetailsResult.data) ? cachedSchoolDetailsResult.data : {};
    
    // Identify teams that need details scraped based on school_url
    const teamsToScrape = finalTeams.filter(team => {
        if (!team.school_url) return false;
        
        const urlKey = getUrlKey(team.school_url);
        if (!urlKey) return false;
        
        // Check if we already have cached data for this URL
        return !unifiedSchoolDetailsCache[urlKey];
    });

    if (teamsToScrape.length > 0) {
        if (Object.keys(unifiedSchoolDetailsCache).length > 0 && cachedSchoolDetailsResult) {
            if (verbose) console.log(`\u001b[36mFound ${Object.keys(unifiedSchoolDetailsCache).length} schools in unified details cache (from ${cachedSchoolDetailsResult.savedAt.toLocaleString()}). Scraping details for ${teamsToScrape.length} new schools.\u001b[0m`);
        } else {
            if (verbose) console.log(`\u001b[36mScraping details for ${teamsToScrape.length} schools.\u001b[0m`);
        }
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
                const detailedData = await scrapeTeamDetailsWithPage(team.school_url, page);
                
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
                    // Store division for teams without accurate division data (backup schools)
                    division: (!team.division || team.division === 'unknown') ? (detailedData?.division ? mapDivisionToStandard(detailedData.division) : null) : null
                };
                results.push(result);
                
                // Update cache immediately after each school is processed
                unifiedSchoolDetailsCache[result.urlKey] = {
                    ...result.details,
                    // Only cache division for backup schools (better than no division)
                    ...(result.division && { division: result.division }),
                    school_url: result.school_url,
                    scraped_at: new Date().toISOString()
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
        for (let i = 0; i < teamsToScrape.length; i += BATCH_SIZE) { batches.push(teamsToScrape.slice(i, i + BATCH_SIZE)); }
        const batchPromises = batches.map((batch, index) => processBatch(batch, browsers[index % CONCURRENT_BROWSERS], index) );    
        const batchResults = await Promise.all(batchPromises);
        // Flatten results (cache was already updated incrementally during processing)
        const newDetails = batchResults.flat();
        
        // Update the main teams cache with division info from detailed scraping
        if (newDetails.length > 0) {
            const teamsData = cacheManager.get("ncaa_schools_backup", NCAA_DETAIL_TTL);
            let currentTeams = teamsData ? teamsData.data : [];
            let teamsUpdated = 0;
            
            // Update teams that got division info from detailed scraping
            currentTeams = currentTeams.map(team => {
                const matchingDetail = newDetails.find(detail => detail.school_url === team.school_url);
                
                if (matchingDetail && matchingDetail.division && (!team.division || team.division === 'unknown')) {
                    if (typeof matchingDetail.division === 'string' && matchingDetail.division.trim()) {
                        teamsUpdated++;
                        return {
                            ...team,
                            division: matchingDetail.division.toLowerCase()
                        };
                    } else if (verbose) {
                        console.log(`\u001b[33mWarning: Invalid division data for team ${team.school_name}: ${typeof matchingDetail.division} - ${matchingDetail.division}\u001b[0m`);
                    }
                }
                return team;
            });
            
            if (teamsUpdated > 0) {
                cacheManager.set("ncaa_schools_backup", currentTeams);
            }
        }
    } else {
        if (verbose && cachedSchoolDetailsResult) console.log(`\u001b[36mUsing cached school details data for all schools (from ${cachedSchoolDetailsResult.savedAt.toLocaleString()}).\u001b[0m`);
    }

    // Update teams cache with divisions from details cache (runs whether we scraped or used cache)
    const teamsData = cacheManager.get("ncaa_schools_backup", NCAA_DETAIL_TTL);
    if (teamsData && teamsData.data && unifiedSchoolDetailsCache) {
        let currentTeams = teamsData.data;
        let teamsUpdated = 0;
        
        // Update teams that have unknown/missing divisions with cached derived divisions
        const updatedTeams = currentTeams.map(team => {
            if (!team.division || team.division === 'unknown') {
                const urlKey = getUrlKey(team.school_url);
                const cachedDetails = urlKey ? unifiedSchoolDetailsCache[urlKey] : null;
                
                if (cachedDetails && cachedDetails.division && typeof cachedDetails.division === 'string' && cachedDetails.division.trim()) {
                    teamsUpdated++;
                    return {
                        ...team,
                        division: cachedDetails.division.toLowerCase()
                    };
                }
            }
            return team;
        });
        
        if (teamsUpdated > 0) {
            cacheManager.set("ncaa_schools_backup", updatedTeams);
            if (verbose) console.log(`\u001b[32mUpdated ${teamsUpdated} teams with derived divisions from details cache\u001b[0m`);
        }
    }
    
    // Check cache for head coach data
    const cachedCoachesResult = cacheManager.get("football_college_coaches", NCAA_COACH_TTL);
    let teamCoachesCache = (cachedCoachesResult && cachedCoachesResult.data) ? cachedCoachesResult.data : {};
    // Identify teams that need head coach data scraped
    const teamsNeedingCoaches = finalTeams.filter(team => team.ncaa_id && !teamCoachesCache[team.ncaa_id]);

    if (teamsNeedingCoaches.length > 0) {
        if (Object.keys(teamCoachesCache).length > 0 && cachedCoachesResult) {
            if (verbose) console.log(`\u001b[36mFound ${Object.keys(teamCoachesCache).length} head coaches in cache (from ${cachedCoachesResult.savedAt.toLocaleString()}). Scraping head coaches for ${teamsNeedingCoaches.length} new teams.\u001b[0m`);
        } else {
            if (verbose) console.log(`\u001b[36mScraping head coaches for ${teamsNeedingCoaches.length} teams.\u001b[0m`);
        }
        // Use all 3 browser instances with session establishment
        const COACH_BATCH_SIZE = Math.ceil(teamsNeedingCoaches.length / CONCURRENT_BROWSERS);
        
        // Function to process a batch of teams for head coach data
        const processCoachBatch = async (teams, browserInstance, batchIndex) => {
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
            
            // First visit the main page to establish session cookies
            try {
                if (verbose) console.log(`${BROWSER_COLORS[batchIndex]}Establishing session by visiting NCAA main page...\u001b[0m`);
                await page.goto('https://stats.ncaa.org/', { waitUntil: 'networkidle0', timeout: 30000 });
                await sleep(3000);
            } catch (error) {
                console.log(`${BROWSER_COLORS[batchIndex]}Warning: Could not establish session on main page, continuing anyway...\u001b[0m`);
            }

            const results = [];
            for (const team of teams) {
                if (verbose) console.log(`${BROWSER_COLORS[batchIndex]}Downloading Head Coach: https://stats.ncaa.org/teams/${team.ncaa_id}\u001b[0m`);
                const headCoach = await scrapeHeadCoachWithPage(team.ncaa_id, page);
                
                results.push({
                    ncaa_id: team.ncaa_id,
                    head_coach: defaultValue(headCoach)
                });
                
                // Update cache immediately after each result (skip null results)
                if (team.ncaa_id && headCoach) {
                    teamCoachesCache[team.ncaa_id] = defaultValue(headCoach);
                    cacheManager.set("football_college_coaches", teamCoachesCache);
                }
                // Rate limiting between requests within the same browser
                await sleep(5000 + Math.random() * 1000);
            }
            await page.close();
            return results;
        };

        // Split teams into batches and process concurrently
        const coachBatches = [];
        for (let i = 0; i < teamsNeedingCoaches.length; i += COACH_BATCH_SIZE) { 
            coachBatches.push(teamsNeedingCoaches.slice(i, i + COACH_BATCH_SIZE)); 
        }
        const coachBatchPromises = coachBatches.map((batch, index) => 
            processCoachBatch(batch, browsers[index % CONCURRENT_BROWSERS], index)
        );
        await Promise.all(coachBatchPromises);
    } else {
        if (verbose && cachedCoachesResult) console.log(`\u001b[36mUsing cached Head Coach data for all teams (from ${cachedCoachesResult.savedAt.toLocaleString()}).\u001b[0m`);
    }
    
    // Check cache for coordinator data
    const cachedCoordinatorsResult = cacheManager.get("football_college_coordinators", NCAA_COACH_TTL);
    let coordinatorCache = (cachedCoordinatorsResult && cachedCoordinatorsResult.data) ? cachedCoordinatorsResult.data : [];
    
    if (coordinatorCache.length === 0) {
        const allCoordinators = [];
        
        // Function to scrape coordinators from a Wikipedia page
        const scrapeCoordinatorsFromWiki = async (url, page) => {
            try {
                await page.goto(url, { waitUntil: 'networkidle0', timeout: 60000 });
                await sleep(3000);
                
                // Get table selector from config
                const tableSelector = CONFIG.ATTRIBUTES[url]?.TABLE;
                if (!tableSelector) {
                    console.log(`\u001b[33mWarning: No table selector found for ${url}\u001b[0m`);
                    return [];
                }
                
                // Extract table data using XPath
                const coordinators = await page.evaluate((xpath) => {
                    const getTableByXPath = (xpath) => {
                        try {
                            const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                            return result.singleNodeValue;
                        } catch (error) {
                            return null;
                        }
                    };
                    
                    const table = getTableByXPath(xpath);
                    if (!table) return [];
                    
                    const rows = table.querySelectorAll('tr');
                    const results = [];
                    let headerRow = null;
                    
                    // Find header row with coordinator columns
                    for (let i = 0; i < rows.length; i++) {
                        const cells = rows[i].querySelectorAll('th, td');
                        const cellTexts = Array.from(cells).map(cell => cell.textContent.trim().toLowerCase());
                        
                        if (cellTexts.some(text => text.includes('team') || text.includes('school')) &&
                            cellTexts.some(text => text.includes('offensive')) &&
                            cellTexts.some(text => text.includes('defensive'))) {
                            headerRow = {
                                teamCol: cellTexts.findIndex(text => text.includes('team') || text.includes('school')),
                                headCoachCol: cellTexts.findIndex(text => text.includes('head coach')),
                                offensiveCol: cellTexts.findIndex(text => text.includes('offensive')),
                                defensiveCol: cellTexts.findIndex(text => text.includes('defensive'))
                            };
                            break;
                        }
                    }
                    
                    if (!headerRow) return [];
                    
                    // Extract data rows
                    for (let i = 1; i < rows.length; i++) {
                        const cells = rows[i].querySelectorAll('td');
                        if (cells.length <= Math.max(headerRow.teamCol, headerRow.offensiveCol, headerRow.defensiveCol)) continue;
                        
                        const teamName = cells[headerRow.teamCol]?.textContent.trim();
                        const headCoach = headerRow.headCoachCol >= 0 ? cells[headerRow.headCoachCol]?.textContent.trim() : null;
                        const offensiveCoordinator = cells[headerRow.offensiveCol]?.textContent.trim();
                        const defensiveCoordinator = cells[headerRow.defensiveCol]?.textContent.trim();
                        
                        if (teamName && teamName.length > 2) {
                            results.push({
                                team: teamName,
                                head_coach: headCoach ? headCoach.replace(/\[[0-9]+\]/g, '').trim() : null,
                                offensive_coordinator: offensiveCoordinator ? offensiveCoordinator.replace(/\[[0-9]+\]/g, '').trim() : null,
                                defensive_coordinator: defensiveCoordinator ? defensiveCoordinator.replace(/\[[0-9]+\]/g, '').trim() : null
                            });
                        }
                    }
                    
                    return results;
                }, tableSelector);
                
                return coordinators;
            } catch (error) {
                console.error(`\u001b[31mFailed to scrape coordinators from ${url}:\u001b[0m`, error.message);
                return [];
            }
        };
        
        // Use existing browser instances - split Wikipedia links across browsers
        const coordBatches = [];
        for (let i = 0; i < CONCURRENT_BROWSERS; i++) { coordBatches.push([]); }
        CONFIG.LINKS.NCAA_COACHES.forEach((link, index) => { 
            coordBatches[index % CONCURRENT_BROWSERS].push(link); 
        });
        
        const coordBatchPromises = coordBatches.map(async (urls, batchIndex) => {
            const browser = browsers[batchIndex];
            const page = await browser.newPage();
            
            const browserConfig = getBrowserConfigWithHeaders({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://en.wikipedia.org/',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin'
            });
            await page.setUserAgent(browserConfig.userAgent);
            await page.setViewport({ width: 1920, height: 1080 });
            await page.setExtraHTTPHeaders(browserConfig.headers);
            
            const batchResults = [];
            for (const url of urls) {
                if (verbose) console.log(`${BROWSER_COLORS[batchIndex]}Downloading NCAA Coordinators: ${url}\u001b[0m`);
                const coordinators = await scrapeCoordinatorsFromWiki(url, page);
                batchResults.push(...coordinators);
                await sleep(2000); // Rate limiting
            }
            
            await page.close();
            return batchResults;
        });
        
        const coordBatchResults = await Promise.all(coordBatchPromises);
        coordBatchResults.forEach(coords => allCoordinators.push(...coords));
        
        coordinatorCache = allCoordinators;
        cacheManager.set("football_college_coordinators", coordinatorCache);
    } else {
        if (verbose) console.log(`\u001b[36mUsing cached coordinator data (${coordinatorCache.length} records from ${cachedCoordinatorsResult.savedAt.toLocaleString()})\u001b[0m`);
    }
    
    // Close all browser instances after all processing is complete
    await Promise.all(browsers.map(browser => browser.close()));
    
    // Function to get cached details by URL
    const getCachedDetailsByUrl = (schoolUrl) => {
        const urlKey = getUrlKey(schoolUrl);
        return urlKey ? unifiedSchoolDetailsCache[urlKey] || null : null;
    };
    
    // Merge cached details and head coaches into all NCAA teams using URL-based matching
    const processedNcaaTeams = finalTeams.map(team => {
        // Get details from unified cache by URL
        const cachedDetails = getCachedDetailsByUrl(team.school_url);
        
        // Get head coach from NCAA ID cache (if available)
        const headCoach = team.ncaa_id ? teamCoachesCache[team.ncaa_id] : null;
        
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
            // Use team division (already includes derived divisions for backup teams)
            division: team.division,
            // Add head coach if available
            head_coach: headCoach || null
        };
        return teamData;
    });

    // NOW MATCH NCAA IDs WITH TEAMS USING CACHED ID DATA
    if (verbose) console.log(`\u001b[32mMatching NCAA IDs with teams using cached ID data...\u001b[0m`);
    const processedNcaaTeamsWithIds = processedNcaaTeams.map(team => {
        if (team.ncaa_id) return team; // Already has an ID
        
        // Try to find matching NCAA ID by team name
        const matchingId = ncaa_ids.find(idRecord => {
            if (!idRecord.team_name || !team.school_name) return false;
            return idRecord.team_name.toLowerCase().trim() === team.school_name.toLowerCase().trim();
        });
        
        if (matchingId) {
            return {
                ...team,
                ncaa_id: matchingId.ncaa_id
            };
        }
        
        return team; // No matching ID found
    });

    // NOW PERFORM ESPN-NCAA MATCHING AFTER ALL SCRAPING IS COMPLETE
    if (verbose) console.log(`\u001b[32mPerforming ESPN-NCAA team matching using hardcoded bindings...\u001b[0m`);
    
    // Load binding model for ESPN-NCAA matching
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
    // Apply hardcoded bindings to match ESPN and NCAA data
    const boundEspnIds = sportBindings.map(b => b.espn_id);
    const boundNcaaIds = sportBindings.map(b => b.ncaa_id);
    const combinedEspn = espnTeams.filter(team => boundEspnIds.includes(String(team.espn_id)));
    const combinedNcaa = processedNcaaTeamsWithIds.filter(team => boundNcaaIds.includes(String(team.ncaa_id)));
    const combinedData = [];
    sportBindings.forEach(binding => {
        const espnTeam = combinedEspn.find(team => String(team.espn_id) === binding.espn_id);
        const ncaaTeam = combinedNcaa.find(team => String(team.ncaa_id) === binding.ncaa_id);
        if (espnTeam && ncaaTeam) {
            combinedData.push({ ...espnTeam, ...ncaaTeam });
        }
    });
    // TODO: Implement ML fallback matching for unbound teams
    // For now, only use hardcoded bindings from the model
    const unboundEspn = espnTeams.filter(team => !boundEspnIds.includes(String(team.espn_id)));
    const unboundNcaa = processedNcaaTeamsWithIds.filter(team => !boundNcaaIds.includes(String(team.ncaa_id)));
    
    /**
     * Matches coordinator data with team records using multiple name matching strategies
     * 
     * @param {string} teamName - The team name to match against coordinator records
     * @param {Array<Object>} coordinatorData - Array of coordinator records from Wikipedia tables
     * @returns {Object|null} Matched coordinator record with offensive/defensive coordinators, or null if no match found
     * 
     */
    const matchCoordinatorByName = (teamName, coordinatorData) => {
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
    
    // Check for local color cache first, then fall back to remote
    const LOCAL_COLOR_CACHE_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days
    const cachedColorsResult = cacheManager.get("football_college_colors", LOCAL_COLOR_CACHE_TTL);
    let colorBindings = [];
    
    if (cachedColorsResult) {
        colorBindings = cachedColorsResult.data;
        if (verbose) console.log(`\u001b[36mUsing local color cache (${colorBindings.length} entries from ${cachedColorsResult.savedAt.toLocaleString()})\u001b[0m`);
    } else {
        // Load initial data from remote CSV, then we'll enhance it locally
        try {
            const bindingsResponse = await fetch('https://github.com/cullenchampagne1/sportsR/releases/download/misc/ncaa_logo_color_bindings.csv');
            const bindingsText = await bindingsResponse.text();
            const lines = bindingsText.split('\n').filter(line => line.trim());
            
            if (lines.length > 1) {
                // Parse header line to find url and colors columns
                const headerLine = lines[0];
                const headers = headerLine.split(',').map(h => h.trim().replace(/"/g, ''));
                const urlIndex = headers.indexOf('url');
                const colorsIndex = headers.indexOf('colors');
                
                if (urlIndex >= 0 && colorsIndex >= 0) {
                    // Parse each data line
                    for (let i = 1; i < lines.length; i++) {
                        const line = lines[i].trim();
                        if (!line) continue;
                        
                        // Split CSV line properly handling quoted values
                        const values = [];
                        let current = '';
                        let inQuotes = false;
                        
                        for (let char of line) {
                            if (char === '"') {
                                inQuotes = !inQuotes;
                            } else if (char === ',' && !inQuotes) {
                                values.push(current.trim());
                                current = '';
                            } else {
                                current += char;
                            }
                        }
                        values.push(current.trim()); // Add the last value
                        
                        // Extract url and colors if they exist
                        if (values.length > Math.max(urlIndex, colorsIndex)) {
                            const url = values[urlIndex] ? values[urlIndex].replace(/"/g, '') : null;
                            const colors = values[colorsIndex] ? values[colorsIndex].replace(/"/g, '') : null;
                            
                            if (url && colors) {
                                colorBindings.push({ url, colors });
                            }
                        }
                    }
                }
            }
            if (verbose) console.log(`\u001b[32mLoaded ${colorBindings.length} remote color bindings as base cache\u001b[0m`);
        } catch (error) {
            if (verbose) console.log(`\u001b[33mWarning: Could not load remote color bindings: ${error.message}\u001b[0m`);
        }
    }

    /**
     * Extracts dominant colors from an image URL
     * 
     * @param {string} imageUrl - URL of the image to process
     * @returns {Promise<string|null>} Comma-separated hex colors or null if extraction fails
     * 
     * @description This function attempts to extract the most dominant colors from an image.
     * It first checks a cache of previously processed images, then falls back to a simple
     * approach using canvas to sample the image colors. In production, this could be enhanced
     * with more sophisticated color extraction algorithms.
     */
    const getDominantColors = async (imageUrl) => {
        if (!imageUrl) return null;
        
        // Check cache first
        const cached = colorBindings.find(binding => binding.url === imageUrl);
        if (cached && cached.colors) {
            return cached.colors;
        }
        
        // Extract colors from SVG by converting to PNG first
        try {
            if (verbose) console.log(`\u001b[32mDownloading Logo Colors: ${imageUrl}\u001b[0m`);
            
            // Download the SVG
            const svgResponse = await fetch(imageUrl);
            if (!svgResponse.ok) {
                throw new Error(`HTTP ${svgResponse.status}`);
            }
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
            
            // Filter out white/black colors similar to R implementation
            const filteredColors = colors.filter(color => 
                color && 
                color.toUpperCase() !== '#FFFFFF' && 
                color.toUpperCase() !== '#000000' &&
                color.toUpperCase() !== '#FEFEFE' && // Near white
                color.toUpperCase() !== '#010101'   // Near black
            );
            
            if (filteredColors.length > 0) {
                const result = filteredColors.join(', ');
                // Update local cache with new colors
                colorBindings.push({ url: imageUrl, colors: result });
                // Save updated cache locally
                cacheManager.set("football_college_colors", colorBindings);
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
    const formatColor = (color) => {
        if (!color || color === 'null' || color === '000000') return null;
        const upperColor = color.toUpperCase();
        return upperColor.startsWith('#') ? upperColor : `#${upperColor}`;
    };
    
    /**
     * Processes combined ESPN and NCAA team data into final structured format
     * 
     * @description This function transforms raw ESPN and NCAA team data into a standardized format
     * that matches the R implementation. It handles:
     * - Team ID generation using crypto hashing
     * - Coordinator data matching from Wikipedia sources
     * - Logo URL prioritization (NCAA over ESPN)
     * - Color extraction and fallback logic
     * - Website URL formatting and validation
     * - Data type standardization and null handling
     * 
     * @param {Array<Object>} combinedData - Array of merged ESPN and NCAA team objects
     * @param {Array<Object>} coordinatorCache - Cached coordinator data from Wikipedia
     * @returns {Promise<Array<Object>>} Array of fully processed team objects with standardized structure
     */
    const processedTeams = await Promise.all(combinedData.map(async (team) => {
        // Try to match coordinator data using different team name variations
        const coordinatorMatch = 
            matchCoordinatorByName(team.displayName, coordinatorCache) ||
            matchCoordinatorByName(team.name, coordinatorCache) ||
            matchCoordinatorByName(team.location, coordinatorCache) ||
            matchCoordinatorByName(team.name_ncaa, coordinatorCache);
        
        // Helper function to generate team ID (similar to R's encode_id function)
        const generateId = (espnId, abv) => {
            if (!espnId) return null;
            
            // Create a consistent hash using ESPN ID as primary identifier
            const hash = crypto.createHash('md5').update(`CFB-${espnId}`).digest('hex');
            
            // Use abbreviation prefix if available, otherwise use hash prefix
            let prefix = '';
            if (abv && abv.length >= 2) {
                // Take first 2 characters of abbreviation, ensure uppercase
                prefix = abv.substring(0, 2).toUpperCase();
            } else {
                // Fallback to first 2 characters of hash if no abbreviation
                prefix = hash.substring(0, 2).toUpperCase();
            }
            
            // Take 6 characters from hash for remainder to make exactly 8 chars total
            const suffix = hash.substring(2, 8).toUpperCase();
            
            return prefix + suffix;
        };
        
        // Use NCAA logo if available, otherwise use ESPN logo
        const logoUrl = team.img_src || team.logo;
        
        // Extract dominant colors from logo if needed
        const dominantColors = await getDominantColors(team.img_src);
        let primaryColor = team.color;
        let secondaryColor = team.alternateColor;
        
        // Use extracted colors as fallback when ESPN colors are missing or black
        if (dominantColors) {
            const colors = dominantColors.split(', ');
            if ((!primaryColor || primaryColor === '000000') && colors[0]) {
                primaryColor = colors[0];
            }
            if ((!secondaryColor || secondaryColor === '000000') && colors[1]) {
                secondaryColor = colors[1];
            }
        }
        
        // Format website URL properly
        let websiteUrl = team.website;
        if (websiteUrl) {
            websiteUrl = websiteUrl.trim();
            if (websiteUrl.toLowerCase() === 'na' || websiteUrl.toLowerCase() === 'n/a' || websiteUrl === '') {
                websiteUrl = null;
            } else {
                websiteUrl = websiteUrl.replace(/[\/\?]+$/, ''); // Remove trailing slashes
                if (!/^https?:\/\//i.test(websiteUrl)) {
                    websiteUrl = `https://${websiteUrl}`;
                }
            }
        }
        
        return {
            id: generateId(team.espn_id, team.abbreviation),
            espn_id: team.espn_id,
            ncaa_id: team.ncaa_id,
            type: "NCAAF",
            slug: team.slug || null,
            abv: team.abbreviation || null,
            full_name: team.displayName || null,
            short_name: team.name || null,
            university: team.name_ncaa || team.location || null,
            division: team.division || null,
            conference: team.conference || null,
            primary: formatColor(primaryColor),
            secondary: formatColor(secondaryColor),
            logo: logoUrl || null,
            head_coach: team.head_coach || coordinatorMatch?.head_coach || null,
            offensive_coordinator: coordinatorMatch?.offensive_coordinator || null,
            defensive_coordinator: coordinatorMatch?.defensive_coordinator || null,
            school_url: team.school_url || null,
            website: websiteUrl,
            twitter: team.twitter || null,
        };
    }));

    
    
    // Save data as JSON - more efficient and maintains data types
    const final_json = JSON.stringify(processedTeams, null, 2);
    if (save) fs.writeFileSync(OUTPUT_FILE, final_json, "utf8");
    if (verbose && save) console.log(`\u001b[90mCollege Football Data Saved To: /data/processed/football-teams-college.json\u001b[0m`);
    
    // Output unmatched teams for debugging team bindings
    if (save) {
        try {
            // Create output directory if it's not exist
            if (!fs.existsSync('output/json')) {
                fs.mkdirSync('output/json', { recursive: true });
            }
            
            // Save unmatched ESPN teams
            const unmatchedEspnTeams = unboundEspn.map(team => ({
                espn_id: team.espn_id,
                name: team.name,
                displayName: team.displayName,
                shortDisplayName: team.shortDisplayName,
                location: team.location,
                abbreviation: team.abbreviation,
                slug: team.slug
            }));
            
            // Save unmatched NCAA teams
            const unmatchedNcaaTeams = unboundNcaa.map(team => ({
                ncaa_id: team.ncaa_id,
                name_ncaa: team.name_ncaa,
                school_name: team.school_name,
                conference_name: team.conference_name,
                logo: team.logo
            }));
            
            fs.writeFileSync('output/json/unmatched-espn-teams.json',JSON.stringify(unmatchedEspnTeams, null, 2), 'utf8');
            fs.writeFileSync('output/json/unmatched-ncaa-teams.json', JSON.stringify(unmatchedNcaaTeams, null, 2), 'utf8');
            
            if (verbose) {
                console.log(`\u001b[90mSaved ${unmatchedEspnTeams.length} unmatched ESPN teams to output/json/unmatched-espn-teams.json\u001b[0m`);
                console.log(`\u001b[90mSaved ${unmatchedNcaaTeams.length} unmatched NCAA teams to output/json/unmatched-ncaa-teams.json\u001b[0m`);
            }
            
        } catch (error) {
            if (verbose) console.error('\u001b[31mError saving unmatched teams files:\u001b[0m', error);
        }
    }
    
    return processedTeams;
}

// Export the function as default
export default get_formated_teams;

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
    get_formated_teams();
}