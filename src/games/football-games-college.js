// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files ("the Software"), to deal in
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
import crypto from 'crypto';
import puppeteer from 'puppeteer';
import ProgressBar from 'progress';

import { getBrowserConfigWithHeaders } from '../util/browser-headers.js';
import { getCanonicalNcaaId, getCacheEntryByCanonicalId } from '../util/ncaa-id-consolidation.js';
import cacheManager from '../util/cache-manager.js';
import get_formated_teams from '../teams/football-teams-college.js';
import { scrapeWithRetry } from '../util/ncaa-school-util.js';

const OUTPUT_FILE = "data/processed/football-games-college.json";
const BASE_CACHE_TTL = 30 * 24 * 60 * 60 * 1000;
const BROWSER_COLORS = ['\u001b[33m', '\u001b[34m', '\u001b[32m'];

/**
 * Calculates cache expiration time based on next upcoming game
 * @param {Array<Object>} games - Array of games with date_time property
 * @returns {number} Expiration timestamp (1 hour after next game, minimum 30 days)
 */
const _calculateExpirationTime = (games) => {
    if (!games?.length) return Date.now() + BASE_CACHE_TTL;
    const now = Date.now();
    const nextGameTime = games
        .filter(game => game.date_time && new Date(game.date_time).getTime() > now)
        .map(game => new Date(game.date_time).getTime())
        .sort((a, b) => a - b)[0];
    return nextGameTime ? nextGameTime + (60 * 60 * 1000) : now + BASE_CACHE_TTL;
};

/**
 * Infers college football week number from game date
 * @param {Date} gameDate - Game date
 * @param {number} season - Season year
 * @returns {number|null} Week number (1-17) or null if invalid
 */
const _inferWeekFromDate = (gameDate, season) => {
    if (!gameDate || !season) return null;
    const daysDiff = Math.floor((gameDate.getTime() - new Date(season, 7, 26).getTime()) / (24 * 60 * 60 * 1000));
    if (daysDiff < 0) return null;
    const week = Math.floor(daysDiff / 7) + 1;
    return week >= 1 && week <= 17 ? week : null;
};

/**
 * Validates team cache data freshness and year coverage
 * @param {Object} teamData - Cached team data with games, savedAt, expiresAt
 * @param {Array<number>} requiredYears - Required years to validate
 * @returns {Object} Cache validity status with missing years info
 */
const _isTeamCacheValid = (teamData, requiredYears) => {
    const defaultResult = { isValid: false, missingYears: requiredYears, needsCurrentYearOnly: false };
    if (!teamData?.savedAt) return defaultResult;
    const getCachedYears = () => new Set((teamData.fetchedYears || teamData.games?.map(g => g.season)) || []);
    const getMissingYears = (cachedYears) => requiredYears.filter(year => !cachedYears.has(year));
    if (Date.now() >= teamData.expiresAt) {
        const cachedYears = new Set(teamData.games?.map(g => g.season) || []);
        const missingYears = getMissingYears(cachedYears);
        const needsCurrentYearOnly = teamData.games?.length > 0;

        return {
            isValid: false,
            missingYears: needsCurrentYearOnly ? [Math.max(...requiredYears)] : missingYears,
            needsCurrentYearOnly
        };
    }
    const cachedYears = getCachedYears();
    const missingYears = getMissingYears(cachedYears);
    return { isValid: missingYears.length === 0, missingYears, needsCurrentYearOnly: false };
};

/**
 * Creates game object from ESPN competition data
 * @param {Object} game - ESPN game data
 * @param {number} year - Season year
 * @returns {Object|null} Game object or null if invalid
 */
const _createEspnGameObject = (game, year) => {
    const comp = game.competitions?.[0];
    if (!comp?.competitors) return null;
    const [homeComp, awayComp] = [comp.competitors.find(c => c.homeAway === 'home'), comp.competitors.find(c => c.homeAway === 'away')];
    if (!homeComp || !awayComp) return null;
    const [homeScore, awayScore] = [homeComp.score?.value || 0, awayComp.score?.value || 0];
    const winner = homeScore > awayScore ? homeComp.team.id : awayScore > homeScore ? awayComp.team.id : null;

    return {
        espn_id: game.id, date_time: game.date, season: year, week: game.week?.number || null,
        title: game.name || null, short_title: game.shortName || null, venue: comp.venue?.fullName || null,
        home_espn_id: homeComp.team.id, away_espn_id: awayComp.team.id,
        home_score: homeScore, away_score: awayScore, winner
    };
};

/**
 * Fetches ESPN schedule data for a team/year
 * @param {string} espnId - ESPN team ID
 * @param {number} year - Season year
 * @param {boolean} verbose - Whether to log progress
 * @returns {Array<Object>} Array of game objects
 */
const _fetchEspnTeamGames = async (espnId, year, verbose) => {
    if (verbose) console.log(`\u001b[32mDownloading Schedule: https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/${espnId}/schedule?season=${year}\u001b[0m`);
    const response = await fetch(`https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/${espnId}/schedule?season=${year}`);
    if (!response.ok) return [];
    const scheduleJson = await response.json();
    if (!scheduleJson?.events) return [];
    const games = scheduleJson.events.map(game => _createEspnGameObject(game, year)).filter(Boolean);
    await new Promise(resolve => setTimeout(resolve, 100));
    return games;
};

/**
 * Process ESPN games data for a single team with caching and progress tracking.
 * @param {string} espnId - ESPN team ID.
 * @param {Object} team - Team data object.
 * @param {Array<number>} yearsToScrape - Years to scrape data for.
 * @param {Object} allSchedulesCache - Cache object for all team schedules.
 * @param {boolean} verbose - Whether to log progress.
 * @param {ProgressBar} [progressBar] - Optional progress bar to update.
 * @param {string} [cacheDate] - Cache date for display in progress bar.
 * @returns {Array} Array of games for the team.
 */
const _processEspnTeamGames = async (espnId, team, yearsToScrape, allSchedulesCache, verbose, progressBar = null, cacheDate = null) => {
    const teamName = team?.short_name || `Team ${espnId}`;
    const teamData = allSchedulesCache[espnId];
    const cacheStatus = _isTeamCacheValid(teamData, yearsToScrape);

    if (cacheStatus.isValid) {
        if (progressBar) progressBar.tick(1, { task: 'ESPN Data Download', status: `${teamName}` });
        return teamData.games.filter(game => yearsToScrape.includes(game.season));
    }
    
    let teamGames = (teamData?.games || []).filter(game => yearsToScrape.includes(game.season));
    const yearsToFetch = cacheStatus.missingYears;
    
    if (yearsToFetch.length > 0) {
        let yearProgress = null;
        if (verbose && yearsToFetch.length > 1) {
            yearProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total years \u001b[90m(:elapseds elapsed)\u001b[0m', {
                total: yearsToFetch.length, width: 40, complete: '=', incomplete: '-', renderThrottle: 100, clear: false, task: `${teamName} - ESPN Years`
            });
        }
        
        teamGames = teamGames.filter(game => !yearsToFetch.includes(game.season));
        for (const year of yearsToFetch) {
            try {
                const newGames = await _fetchEspnTeamGames(espnId, year, false);
                teamGames.push(...newGames);
                if (yearProgress) yearProgress.tick(1, { task: `${teamName} - ESPN Years`, status: `${year} (${newGames.length} games)` });
            } catch (error) {
                if (yearProgress) yearProgress.tick(1, { task: `${teamName} - ESPN Years`, status: `${year} (ERROR: ${error.message})` });
            }
        }
        if (yearProgress) yearProgress.terminate();
    }
    
    const existingFetchedYears = new Set(teamData?.fetchedYears || []);
    yearsToFetch.forEach(year => existingFetchedYears.add(year));
    const allFetchedYears = [...existingFetchedYears, ...(teamData?.games?.map(g => g.season) || [])];
    
    allSchedulesCache[espnId] = {
        games: teamGames,
        savedAt: Date.now(),
        expiresAt: _calculateExpirationTime(teamGames),
        fetchedYears: [...new Set(allFetchedYears)]
    };
    cacheManager.set("football_college_espn_schedules", allSchedulesCache);
    
    if (progressBar) progressBar.tick(1, { task: 'ESPN Data Download', status: `${teamName} - ${teamGames.length} games (${yearsToFetch.length} years fetched)` });
    return teamGames;
};

/**
 * Scrape NCAA schedule for a team with session establishment.
 * @param {string} ncaaId - NCAA team ID.
 * @param {Array<number>} years - Years to scrape.
 * @param {Object} page - Puppeteer page instance.
 * @param {boolean} verbose - Whether to log progress.
 * @param {number} pageIndex - Browser page index for colored logging.
 * @returns {Array} Array of games with NCAA opponent IDs.
 */
const _scrapeNcaaTeamSchedule = async (ncaaId, years, page, verbose, pageIndex = 0) => {
    const log = (msg, color = BROWSER_COLORS[pageIndex]) => { if (verbose) console.log(`${color}${msg}\u001b[0m`); };
    const teamUrl = `https://stats.ncaa.org/teams/${ncaaId}`;
    log(`Visiting NCAA team page: ${teamUrl}`);

    const scrapeAction = async (page) => {
        const games = [];
        const yearOptions = await page.$eval('#year_list', (select, targetYears) =>
            Array.from(select.options)
                .map(option => ({ value: option.value, text: option.text }))
                .filter(option => targetYears.includes(parseInt(option.text.split('-')[0]))), years);

        for (const yearOption of yearOptions) {
            try {
                log(`Processing year ${yearOption.text} for NCAA ID ${ncaaId}`);
                await page.select('#year_list', yearOption.value);
                await page.waitForNavigation({ waitUntil: 'networkidle2' });
                const yearGames = await page.evaluate((currentNcaaId, seasonYear) => {
                    const yearGames = [];
                    const scheduleTable = Array.from(document.querySelectorAll('table')).find(table => table.querySelector('tr > td, tr > th')?.textContent.trim().toLowerCase() === 'date');
                    if (!scheduleTable) return [];
                    const rows = Array.from(scheduleTable.querySelectorAll('tr')).slice(1);
                    for (const row of rows) {
                        const cells = row.querySelectorAll('td');
                        if (cells.length < 3) continue;
                        const opponentLink = cells[1]?.querySelector('a[href*="/teams/"]');
                        const boxScoreLink = cells[2]?.querySelector('a[href*="/contests/"]');
                        const rawOpponentNcaaId = opponentLink?.href.match(/\/teams\/(\d+)/)?.[1] || null;

                        yearGames.push({
                            date: cells[0]?.textContent.trim() || '',
                            opponent_name: cells[1]?.textContent.trim() || '',
                            opponent_ncaa_id: rawOpponentNcaaId,
                            ncaa_game_id: boxScoreLink?.href.match(/\/contests\/(\d+)/)?.[1] || null,
                            score: cells[2]?.textContent.trim() || '',
                            season: seasonYear,
                            home_team_ncaa_id: currentNcaaId
                        });
                    }
                    return yearGames;
                }, ncaaId, parseInt(yearOption.text.split('-')[0]));
                
                const consolidatedYearGames = yearGames.map(game => ({
                    ...game,
                    opponent_ncaa_id: game.opponent_ncaa_id ? getCanonicalNcaaId(game.opponent_ncaa_id, 'football') : null,
                    home_team_ncaa_id: getCanonicalNcaaId(game.home_team_ncaa_id, 'football')
                }));
                games.push(...consolidatedYearGames);
            } catch (yearError) { log(`Warning: Failed to process year ${yearOption.text} for NCAA ID ${ncaaId}: ${yearError.message}`, '\u001b[33m'); }
        }
        return games;
    };
    const scrapedGames = await scrapeWithRetry(page, teamUrl, scrapeAction, verbose);
    return scrapedGames || [];
};

/**
 * Processes NCAA schedules for a list of team IDs, scraping data for teams with an invalid cache.
 * @param {Array<string>} ncaaIds - Array of unique NCAA team IDs to process.
 * @param {Array<Object>} teamsData - The full list of team data.
 * @param {Object} cache - The cache object for NCAA schedules.
 * @param {Array<number>} yearsToScrape - The years to scrape data for.
 * @param {Array<Object>} pages - The pool of Puppeteer page instances.
 * @param {boolean} verbose - Whether to log progress.
 */
const _processNcaaSchedules = async (ncaaIds, teamsData, cache, yearsToScrape, pages, verbose) => {
    const teamsToProcess = ncaaIds.filter(id => !_isTeamCacheValid(getCacheEntryByCanonicalId(cache, id, 'football'), yearsToScrape).isValid);
    if (pages.length === 0 || teamsToProcess.length === 0) return;
    
    let ncaaProgress = null;
    if (verbose && teamsToProcess.length > 0) {
        process.stdout.write('\u001b[1A');
        ncaaProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: teamsToProcess.length, width: 40, complete: '=', incomplete: '-', renderThrottle: 100, clear: false, task: 'NCAA Data Scraping'
        });
    }

    let pageIndex = 0;
    for (const ncaaId of teamsToProcess) {
        const page = pages[pageIndex % pages.length];
        const team = teamsData.find(t => t.reference_id === ncaaId);
        const teamName = team?.short_name || `NCAA ${ncaaId}`;
        const teamCacheData = getCacheEntryByCanonicalId(cache, ncaaId, 'football');
        const cacheStatus = _isTeamCacheValid(teamCacheData, yearsToScrape);
        const yearsToFetch = cacheStatus.missingYears.length > 0 ? cacheStatus.missingYears : yearsToScrape;
        try {
            const newGames = await _scrapeNcaaTeamSchedule(ncaaId, yearsToFetch, page, false, pageIndex % pages.length);
            const existingGames = (teamCacheData?.games || []).filter(game => !yearsToFetch.includes(game.season));
            const allGames = [...existingGames, ...newGames];
            const gamesWithDateTime = allGames.map(g => ({ date_time: _parseNcaaDate(g.date)?.toISOString() || null }));
            const existingFetchedYears = new Set(teamCacheData?.fetchedYears || []);
            yearsToFetch.forEach(year => existingFetchedYears.add(year));
            const allGameYears = allGames.map(g => g.season);
            const allFetchedYears = [...existingFetchedYears, ...allGameYears];
            
            cache[ncaaId] = {
                games: allGames,
                savedAt: Date.now(),
                expiresAt: _calculateExpirationTime(gamesWithDateTime),
                fetchedYears: [...new Set(allFetchedYears)]
            };

            if (ncaaProgress) ncaaProgress.tick(1, { task: 'NCAA Data Scraping', status: `${teamName} - ${newGames.length} games` });
        } catch (error) {
            if (ncaaProgress) ncaaProgress.tick(1, { task: 'NCAA Data Scraping', status: `${teamName} - ERROR: ${error.message}` });
        }
        cacheManager.set("football_college_ncaa_schedules", cache);
        pageIndex++;
        await new Promise(resolve => setTimeout(resolve, 3000));
    }

    if (ncaaProgress) ncaaProgress.terminate();
    cacheManager.set("football_college_ncaa_schedules", cache);
};

/**
 * Determines if an ESPN game and an NCAA game are a valid match based on date proximity and team IDs.
 * @param {Date} espnDate - The date of the ESPN game.
 * @param {Date} ncaaDate - The date of the NCAA game.
 * @param {Object} homeTeam - The home team object from the consolidated data.
 * @param {Object} awayTeam - The away team object from the consolidated data.
 * @param {Object} ncaaGame - The NCAA game object being evaluated.
 * @returns {boolean} True if the games are considered a match, false otherwise.
 */
const _isMatch = (espnDate, ncaaDate, homeTeam, awayTeam, ncaaGame) => {
    const daysDifference = Math.abs((espnDate - ncaaDate) / (1000 * 60 * 60 * 24));
    if (daysDifference > 7) return false;
    const availableNcaaIds = [homeTeam?.reference_id, awayTeam?.reference_id].filter(Boolean);
    if (availableNcaaIds.length === 0) return false;
    const espnTeamNcaaIds = new Set(availableNcaaIds);
    if (daysDifference <= 1.5) return espnTeamNcaaIds.has(ncaaGame.home_team_ncaa_id) || espnTeamNcaaIds.has(ncaaGame.opponent_ncaa_id);
    if (availableNcaaIds.length === 2) return espnTeamNcaaIds.has(ncaaGame.home_team_ncaa_id) && espnTeamNcaaIds.has(ncaaGame.opponent_ncaa_id);
    return espnTeamNcaaIds.has(ncaaGame.home_team_ncaa_id) || espnTeamNcaaIds.has(ncaaGame.opponent_ncaa_id);
};

/**
 * Parses NCAA date string to Date object
 * @param {string} dateStr - Date string to parse
 * @returns {Date|null} Parsed date or null if invalid
 */
const _parseNcaaDate = (dateStr) => {
    if (!dateStr) return null;
    // Handle MM/DD/YYYY format
    if (dateStr.includes('/')) {
        const [month, day, year] = dateStr.split(' ')[0].split('/').map(Number);
        if ([month, day, year].some(isNaN)) return null;
        const date = new Date(year, month - 1, day);
        return date.getMonth() + 1 === month ? date : null;
    }
    // Handle other formats
    const date = new Date(dateStr);
    return isNaN(date.getTime()) ? null : date;
};

/**
 * Parses score string and determines winner from team perspective
 * @param {string} winLossIndicator - W/L/T indicator or null
 * @param {string} gameTeamId - Team ID from whose perspective the score is
 * @param {string} homeTeamId - Home team ID
 * @param {string} awayTeamId - Away team ID
 * @returns {string|null} Winner team ID or null
 */
const _determineWinnerFromIndicator = (winLossIndicator, gameTeamId, homeTeamId, awayTeamId) => {
    if (!winLossIndicator || winLossIndicator === 'T') return null;
    const isGameTeamHome = gameTeamId === homeTeamId;
    const gameTeamWon = winLossIndicator === 'W';
    return isGameTeamHome ? (gameTeamWon ? homeTeamId : awayTeamId) : (gameTeamWon ? awayTeamId : homeTeamId);
};

/**
 * Parses NCAA score string to extract game results
 * @param {string} scoreStr - Score string (e.g., "W 41-20", "L 20-41", "T 14-14")
 * @param {string} homeTeamId - Home team ID
 * @param {string} awayTeamId - Away team ID
 * @param {string} gameTeamId - Team ID this score belongs to (perspective)
 * @returns {Object} Score data with home_score, away_score, winner_id
 */
const _parseNcaaScore = (scoreStr, homeTeamId, awayTeamId, gameTeamId) => {
    const result = { home_score: null, away_score: null, winner_id: null };
    if (!scoreStr) return result;
    const trimmed = scoreStr.trim();
    const winLossMatch = trimmed.match(/^([WLT])\s+(.+)$/);
    const [scoresPart, winLossIndicator] = [winLossMatch ? winLossMatch[2] : trimmed, winLossMatch ? winLossMatch[1] : null];

    // Extract scores
    const scoreMatch = scoresPart.match(/(\d+)-(\d+)/);
    if (!scoreMatch) return result;

    const [, score1Str, score2Str] = scoreMatch;
    const score1 = parseInt(score1Str);
    const score2 = parseInt(score2Str);

    // Assign scores based on team perspective
    const isGameTeamHome = gameTeamId === homeTeamId;
    if (isGameTeamHome) {
        result.home_score = score1;
        result.away_score = score2;
    } else {
        result.away_score = score1;
        result.home_score = score2;
    }

    // Determine winner
    result.winner_id = _determineWinnerFromIndicator(winLossIndicator, gameTeamId, homeTeamId, awayTeamId);

    // If no indicator, determine from scores
    if (!winLossIndicator && result.home_score !== null && result.away_score !== null) {
        if (result.home_score > result.away_score) result.winner_id = homeTeamId;
        else if (result.away_score > result.home_score) result.winner_id = awayTeamId;
    }
    return result;
};

/**
 * Cleans NCAA opponent name by removing location indicators
 * @param {string} opponentName - Raw opponent name from NCAA data
 * @returns {string} Cleaned team name
 */
const _cleanNcaaOpponentName = (opponentName) => {
    if (!opponentName) return 'Unknown';

    const cleaned = opponentName
        .trim()
        .replace(/^@\s*/, '') // Remove leading @ symbol
        .replace(/\s+@[^,]+(?:,\s*[A-Z]{2})?$/, '') // Remove location indicators
        .trim();

    return cleaned || 'Unknown';
};

/**
 * Common suffixes to filter from team names when generating abbreviations
 */
const TEAM_SUFFIXES = [
    'University', 'College', 'State', 'Tech', 'Institute', 'School',
    'Aggies', 'Bulldogs', 'Tigers', 'Eagles', 'Lions', 'Bears', 'Wildcats',
    'Cardinals', 'Panthers', 'Spartans', 'Warriors', 'Knights', 'Falcons',
    'Hawks', 'Rams', 'Bulls', 'Hornets', 'Cougars', 'Mustangs', 'Broncos'
];

/**
 * Filters team name words by removing common suffixes
 * @param {Array<string>} words - Array of words from team name
 * @returns {Array<string>} Filtered words
 */
const _filterTeamWords = (words) => {
    const filtered = words.filter(word => !TEAM_SUFFIXES.some(suffix => word.toLowerCase() === suffix.toLowerCase()));
    return filtered.length > 0 ? filtered : words;
};

/**
 * Generates abbreviation from words using different strategies
 * @param {Array<string>} words - Team name words
 * @returns {string} Generated abbreviation
 */
const _buildAbbreviation = (words) => {
    if (words.length === 1) return words[0].replace(/\./g, '').substring(0, 4);
    if (words.length === 2) return words[0].substring(0, 2) + words[1].substring(0, 2);
    return words.slice(0, 4).map(word => word.charAt(0)).join('');
};

/**
 * Generates team abbreviation from team name
 * @param {string} teamName - Full team name
 * @returns {string} 3-4 character abbreviation
 */
const _generateTeamAbbreviation = (teamName) => {
    if (!teamName || teamName === 'Unknown') return 'UNK';

    const cleaned = teamName.trim();
    if (cleaned.length <= 4) return cleaned.toUpperCase();

    const words = cleaned.split(/\s+/);
    const filteredWords = _filterTeamWords(words);

    const abbreviation = _buildAbbreviation(filteredWords)
        .replace(/[.&]/g, '') // Remove dots and ampersands
        .toUpperCase();

    // Ensure minimum length of 3
    return abbreviation.length >= 3 ? abbreviation :
           cleaned.substring(0, 3).toUpperCase();
};

/**
 * Match ESPN games with NCAA games to add reference IDs and internal team IDs.
 * @param {Array} espnGames - Array of ESPN games.
 * @param {Object} ncaaSchedulesCache - NCAA schedules cache.
 * @param {Array} teamsData - Teams data with ESPN and NCAA IDs.
 * @param {Array<number>} yearsToScrape - Years being processed.
 * @param {boolean} verbose - Whether to log progress.
 * @returns {Array} Array of ESPN games with NCAA references added.
 */
const _matchEspnWithNcaaGames = async (espnGames, ncaaSchedulesCache, teamsData, yearsToScrape, verbose) => {
    const { findTeamByName } = await import('../util/ncaa-id-consolidation.js');
    const now = new Date();
    const log = (msg, color = '\u001b[36m') => { if (verbose) console.log(`${color}${msg}\u001b[0m`); };

    const allNcaaGames = Object.values(ncaaSchedulesCache).flatMap(cache => (cache?.games || []).filter(game => yearsToScrape.includes(game.season)));
    const ncaaGames = Object.values(allNcaaGames.reduce((acc, game) => { if (game.ncaa_game_id && !acc[game.ncaa_game_id]) acc[game.ncaa_game_id] = game; return acc; }, {}));

    const completedEspnGames = espnGames.filter(game => new Date(game.date_time) < now);
    log(`Matching ${completedEspnGames.length} completed ESPN games with ${ncaaGames.length} NCAA games...`, '\u001b[32m');

    let skippedDuplicateMatches = 0;
    const matchedNcaaGameIds = new Set();

    completedEspnGames.forEach(espnGame => {
        if (espnGame.reference_id) return;

        const espnDate = new Date(espnGame.date_time);
        const title = espnGame.title || espnGame.short_title || '';
        const teams = title.split(/\s(?:vs|at|@)\s/i);
        let homeTeam = teamsData.find(t => t.espn_id === espnGame.home_espn_id) || (teams.length === 2 ? findTeamByName(teams[1], teamsData) : null);
        let awayTeam = teamsData.find(t => t.espn_id === espnGame.away_espn_id) || (teams.length === 2 ? findTeamByName(teams[0], teamsData) : null);

        const ncaaMatch = ncaaGames.find(ncaaGame => {
            const ncaaDate = _parseNcaaDate(ncaaGame.date);
            if (!ncaaGame.ncaa_game_id || !ncaaDate) return false;
            if (matchedNcaaGameIds.has(ncaaGame.ncaa_game_id)) {
                if (_isMatch(espnDate, ncaaDate, homeTeam, awayTeam, ncaaGame)) skippedDuplicateMatches++;
                return false;
            }
            return _isMatch(espnDate, ncaaDate, homeTeam, awayTeam, ncaaGame);
        });

        if (ncaaMatch) {
            espnGame.reference_id = ncaaMatch.ncaa_game_id;
            matchedNcaaGameIds.add(ncaaMatch.ncaa_game_id);
        }
    });

    espnGames.forEach(game => {
        const title = game.title || game.short_title || '';
        const teams = title.split(/\s(?:vs|at|@)\s/i);
        let homeTeam = teamsData.find(t => t.espn_id === game.home_espn_id) || (teams.length === 2 ? findTeamByName(teams[1], teamsData) : null);
        let awayTeam = teamsData.find(t => t.espn_id === game.away_espn_id) || (teams.length === 2 ? findTeamByName(teams[0], teamsData) : null);

        if (homeTeam) { 
            game.home_id = homeTeam.id; 
            game.home = homeTeam.short_name;
            game.home_team_name = homeTeam.short_name;
        } else game.home_team_name = "Unknown";
        
        if (awayTeam) { 
            game.away_id = awayTeam.id; 
            game.away = awayTeam.short_name;
            game.away_team_name = awayTeam.short_name;
        } else game.away_team_name = "Unknown";

        if (game.winner) {
            if (game.winner === game.home_espn_id) game.winner = homeTeam?.id;
            else if (game.winner === game.away_espn_id) game.winner = awayTeam?.id;
        }
    });

    const totalMatched = espnGames.filter(g => g.reference_id).length;
    log(`Matched ${totalMatched} total games out of ${completedEspnGames.length} completed games`, '\u001b[36m');
    if (skippedDuplicateMatches > 0) log(`Prevented ${skippedDuplicateMatches} duplicate NCAA game ID matches`, '\u001b[33m');

    return espnGames;
};

/**
 * Processes ESPN-to-NCAA binding logic
 * @param {Array} espnGames - ESPN games data
 * @param {Array} allNcaaGames - All NCAA games data
 * @param {Array} teamsData - Teams data
 * @param {Map} potentialBindingsMap - Map to store potential bindings
 */
const _processEspnToNcaaBindings = (espnGames, allNcaaGames, teamsData, potentialBindingsMap) => {
    espnGames.forEach(espnGame => {
        if (!espnGame.date_time || espnGame.reference_id) return;

        const homeTeam = teamsData.find(t => t.espn_id === espnGame.home_espn_id);
        const awayTeam = teamsData.find(t => t.espn_id === espnGame.away_espn_id);
        const isHomeUnbound = !homeTeam && !!awayTeam?.reference_id;
        const isAwayUnbound = !awayTeam && !!homeTeam?.reference_id;

        if (!isHomeUnbound && !isAwayUnbound) return;

        const boundTeam = isHomeUnbound ? awayTeam : homeTeam;
        const unboundEspnId = isHomeUnbound ? espnGame.home_espn_id : espnGame.away_espn_id;
        const espnDate = new Date(espnGame.date_time);

        const matchingNcaaGames = allNcaaGames.filter(ncaaGame => {
            const ncaaDate = _parseNcaaDate(ncaaGame.date);
            const involvesBoundTeam = ncaaGame.home_team_ncaa_id === boundTeam.reference_id || ncaaGame.opponent_ncaa_id === boundTeam.reference_id;
            return ncaaDate && involvesBoundTeam && Math.abs((espnDate - ncaaDate) / (1000 * 60 * 60 * 24)) < 2.5;
        });

        matchingNcaaGames.forEach(ncaaGame => {
            const opponentNcaaId = ncaaGame.home_team_ncaa_id === boundTeam.reference_id ? ncaaGame.opponent_ncaa_id : ncaaGame.home_team_ncaa_id;
            if (!opponentNcaaId) return;

            const bindingKey = `espn-${unboundEspnId}-${opponentNcaaId}`;
            const existingBinding = potentialBindingsMap.get(bindingKey) || {
                type: 'espn_to_ncaa',
                espn_id: unboundEspnId, ncaa_id: opponentNcaaId, evidence_games: [], confidence: 0,
                bound_team_name: boundTeam.short_name, espn_game_title: espnGame.title
            };

            existingBinding.evidence_games.push({ espn_game_id: espnGame.espn_id, ncaa_game_id: ncaaGame.ncaa_game_id });
            existingBinding.confidence = existingBinding.evidence_games.length;
            const existingTeamWithNcaaId = teamsData.find(team => team.reference_id === opponentNcaaId);
            Object.assign(existingBinding, {
                already_bound: !!existingTeamWithNcaaId,
                existing_espn_id: existingTeamWithNcaaId?.espn_id || null
            });
            potentialBindingsMap.set(bindingKey, existingBinding);
        });
    });
};

/**
 * Creates a name-based lookup map for teams
 * @param {Array} teamsData - Teams data
 * @returns {Map} Map of team names to team objects
 */
const _createTeamsNameMap = (teamsData) => {
    return teamsData.reduce((map, team) => {
        [team.short_name, team.full_name, team.university]
            .filter(Boolean)
            .forEach(name => map.set(name.toLowerCase().trim(), team));
        return map;
    }, new Map());
};

/**
 * Processes NCAA consolidation logic
 * @param {Array} allNcaaGames - All NCAA games data
 * @param {Array} teamsData - Teams data
 * @param {Map} potentialBindingsMap - Map to store potential bindings
 */
const _processNcaaConsolidations = (allNcaaGames, teamsData, potentialBindingsMap) => {
    const teamsByName = _createTeamsNameMap(teamsData);

    // Helper function to process potential consolidations
    const processPotentialConsolidation = (foundTeam, missingNcaaId, missingTeamName, ncaaGame) => {
        if (!foundTeam || !missingNcaaId || !missingTeamName) return;

        // Skip if this NCAA ID is already consolidated
        const canonicalId = getCanonicalNcaaId(missingNcaaId, 'football');
        if (canonicalId !== missingNcaaId) return;

        // Try to find the missing team by name matching
        const cleanMissingName = missingTeamName.toLowerCase().trim();
        const potentialMatch = teamsByName.get(cleanMissingName);

        if (potentialMatch && potentialMatch.reference_id !== missingNcaaId) {
            const bindingKey = `ncaa-consolidation-${missingNcaaId}-${potentialMatch.reference_id}`;
            const existingBinding = potentialBindingsMap.get(bindingKey) || {
                type: 'ncaa_consolidation',
                source_ncaa_id: missingNcaaId,
                target_ncaa_id: potentialMatch.reference_id,
                team_name: missingTeamName,
                evidence_games: [],
                confidence: 0,
                matched_team: potentialMatch
            };

            existingBinding.evidence_games.push({ ncaa_game_id: ncaaGame.ncaa_game_id, opponent_team: foundTeam.short_name });
            existingBinding.confidence = existingBinding.evidence_games.length;
            potentialBindingsMap.set(bindingKey, existingBinding);
        }
    };

    allNcaaGames.forEach(ncaaGame => {
        if (!ncaaGame.home_team_ncaa_id || !ncaaGame.opponent_ncaa_id) return;

        const homeTeam = teamsData.find(t => t.reference_id === ncaaGame.home_team_ncaa_id);
        const awayTeam = teamsData.find(t => t.reference_id === ncaaGame.opponent_ncaa_id);

        // Check home team missing, away team found
        if (!homeTeam && awayTeam && ncaaGame.home_team_name) {
            processPotentialConsolidation(awayTeam, ncaaGame.home_team_ncaa_id, ncaaGame.home_team_name, ncaaGame);
        }

        // Check away team missing, home team found
        if (homeTeam && !awayTeam && ncaaGame.opponent_name) {
            processPotentialConsolidation(homeTeam, ncaaGame.opponent_ncaa_id, ncaaGame.opponent_name, ncaaGame);
        }
    });
};

/**
 * Deduces potential ESPN-to-NCAA team bindings from games where one team is matched and the other is not,
 * and also identifies NCAA-only team ID consolidation mappings where teams exist under different IDs.
 * @param {Array} espnGames - Array of processed ESPN games.
 * @param {Object} ncaaSchedulesCache - The cache of NCAA schedules.
 * @param {Array} teamsData - The current consolidated teams data.
 * @param {Array<number>} yearsToScrape - The years being processed.
 * @returns {Array} An array of potential binding objects.
 */
const deducePotentialBindings = (espnGames, ncaaSchedulesCache, teamsData, yearsToScrape) => {
    const potentialBindingsMap = new Map();
    const allNcaaGames = Object.values(ncaaSchedulesCache).flatMap(teamCache => (teamCache.games || []).filter(game => yearsToScrape.includes(game.season)));
    // Process ESPN-to-NCAA bindings
    _processEspnToNcaaBindings(espnGames, allNcaaGames, teamsData, potentialBindingsMap);
    // Process NCAA consolidations
    _processNcaaConsolidations(allNcaaGames, teamsData, potentialBindingsMap);
    return Array.from(potentialBindingsMap.values());
};

/**
 * Processes unmatched ESPN games for reporting
 * @param {Array} allGames - All games data
 * @param {Array} teamsData - Teams data
 * @returns {Array} Unmatched ESPN games
 */
const _processUnmatchedEspnGames = (allGames, teamsData) => {
    const now = new Date();
    return allGames
        .filter(game => new Date(game.date_time) < now && !game.reference_id && game.espn_id)
        .map(game => ({
            espn_id: game.espn_id, date_time: game.date_time, title: game.title,
            home_espn_id: game.home_espn_id, away_espn_id: game.away_espn_id,
            home_team: teamsData.find(t => t.espn_id === game.home_espn_id)?.short_name || 'Unknown',
            away_team: teamsData.find(t => t.espn_id === game.away_espn_id)?.short_name || 'Unknown',
            status: 'ESPN game with no NCAA correlation'
        }));
};

/**
 * Processes unmatched NCAA games for reporting
 * @param {Array} validGames - Valid games data
 * @param {Array} teamsData - Teams data
 * @returns {Array} Unmatched NCAA games
 */
const _processUnmatchedNcaaGames = (validGames, teamsData) => {
    return validGames
        .filter(game => !game.espn_id && game.reference_id)
        .map(game => ({
            ncaa_game_id: game.reference_id, date_time: game.date_time, title: game.title,
            home_ncaa_id: game.home_ncaa_id, away_ncaa_id: game.away_ncaa_id,
            home_id: game.home_id, away_id: game.away_id,
            home_team: teamsData.find(t => t.id === game.home_id)?.short_name || 'Unknown',
            away_team: teamsData.find(t => t.id === game.away_id)?.short_name || 'Unknown',
            status: 'NCAA game with no ESPN correlation'
        }));
};

/**
 * Processes filtered team games for reporting
 * @param {Array} filteredGames - Games that were filtered out
 * @param {Array} teamsData - Teams data
 * @returns {Array} Processed filtered games
 */
const _processFilteredTeamGames = (filteredGames, teamsData) => {
    return filteredGames.map(game => {
        const [homeTeam, awayTeam] = [
            teamsData.find(t => t.espn_id === game.home_espn_id || t.reference_id === game.home_ncaa_id),
            teamsData.find(t => t.espn_id === game.away_espn_id || t.reference_id === game.away_ncaa_id)
        ];
        const [homeIssue, awayIssue] = [!game.home_id || game.home_id === 'MISSING', !game.away_id || game.away_id === 'MISSING'];
        const getFailureReason = (issue, team, espnId, ncaaId) => {
            if (!issue) return '';
            if (espnId && !team) return 'ESPN ID not found in teams database';
            if (ncaaId && !team) return 'NCAA ID not found in teams database';
            return 'No team identification available';
        };
        const [homeFailureReason, awayFailureReason] = [
            getFailureReason(homeIssue, homeTeam, game.home_espn_id, game.home_ncaa_id),
            getFailureReason(awayIssue, awayTeam, game.away_espn_id, game.away_ncaa_id)
        ];

        return {
            espn_id: game.espn_id || null,
            ncaa_game_id: game.reference_id || null,
            date_time: game.date_time,
            title: game.title,
            game_type: game.espn_id ? (game.reference_id ? 'ESPN+NCAA' : 'ESPN-only') : 'NCAA-only',
            home_espn_id: game.home_espn_id || null,
            home_ncaa_id: game.home_ncaa_id || null,
            home_issue: homeIssue,
            home_failure_reason: homeFailureReason,
            away_espn_id: game.away_espn_id || null,
            away_ncaa_id: game.away_ncaa_id || null,
            away_issue: awayIssue,
            away_failure_reason: awayFailureReason
        };
    });
};

/**
 * Writes report files and logs statistics
 * @param {Array} unmatchedEspnGames - Unmatched ESPN games
 * @param {Array} unmatchedNcaaGames - Unmatched NCAA games
 * @param {Array} filteredTeamGames - Filtered team games
 * @param {Array} potentialBindings - Potential bindings
 * @param {boolean} verbose - Whether to log progress
 */
const _writeReportsAndStats = (unmatchedEspnGames, unmatchedNcaaGames, filteredTeamGames, potentialBindings, verbose) => {
    // Only write files if they contain data
    if (unmatchedEspnGames.length > 0) fs.writeFileSync('output/unmatched-espn-football-games.json', JSON.stringify(unmatchedEspnGames, null, 2));
    if (unmatchedNcaaGames.length > 0) fs.writeFileSync('output/unmatched-ncaa-football-games.json', JSON.stringify(unmatchedNcaaGames, null, 2));
    if (filteredTeamGames.length > 0) fs.writeFileSync('output/filtered-football-games.json', JSON.stringify(filteredTeamGames, null, 2));

    const espnIdIssues = filteredTeamGames.filter(g => g.home_failure_reason.includes('ESPN ID') || g.away_failure_reason.includes('ESPN ID')).length;
    const ncaaIdIssues = filteredTeamGames.filter(g => g.home_failure_reason.includes('NCAA ID') || g.away_failure_reason.includes('NCAA ID')).length;
    const noIdIssues = filteredTeamGames.filter(g => g.home_failure_reason.includes('No team identification') || g.away_failure_reason.includes('No team identification')).length;

    if (verbose) {
        console.log(`\u001b[90mSaved ${unmatchedEspnGames.length} unmatched ESPN games, ${unmatchedNcaaGames.length} unmatched NCAA games, and ${filteredTeamGames.length} filtered games.\u001b[0m`);
        console.log(`\u001b[90mFiltered game issues: ${espnIdIssues} ESPN ID issues, ${ncaaIdIssues} NCAA ID issues, ${noIdIssues} no identification issues\u001b[0m`);
    }

    if (potentialBindings.length > 0) {
        const csvHeader = 'espn_id,ncaa_id,sport,espn_game_title,bound_team_name,confidence,already_bound,existing_espn_id';
        const csvRows = potentialBindings.map(b => `${b.espn_id},${b.ncaa_id},football,"${b.espn_game_title}","${b.bound_team_name}",${b.confidence},${b.already_bound},${b.existing_espn_id || ''}`);
        fs.writeFileSync('output/csv/new-potential-binds.csv', [csvHeader, ...csvRows].join('\n'), 'utf8');
        if (verbose) console.log(`\u001b[32mGenerated ${potentialBindings.length} potential bindings in output/csv/new-potential-binds.csv\u001b[0m`);
    }
};


/**
 * Saves reports of unmatched games and potential new team bindings.
 * @param {Array} validGames - The final array of games that made it to the dataset.
 * @param {Array} allGames - The complete array of processed games before filtering.
 * @param {Object} ncaaSchedulesCache - The cache of NCAA schedules.
 * @param {Array} teamsData - The current consolidated teams data.
 * @param {Array} potentialBindings - Deduced potential bindings.
 * @param {boolean} verbose - Whether to log progress.
 */
const _saveUnmatchedReports = (validGames, allGames, teamsData, potentialBindings, verbose) => {
    try {
        fs.mkdirSync('output/csv', { recursive: true });

        // Find games that were filtered out (in allGames but not in validGames)
        const validGameIds = new Set(validGames.map(g => g.espn_id || g.reference_id || g.id));
        const filteredGames = allGames.filter(game => !validGameIds.has(game.espn_id || game.reference_id || game.id));

        // Process different types of unmatched games
        const unmatchedEspnGames = _processUnmatchedEspnGames(allGames, teamsData);
        const unmatchedNcaaGames = _processUnmatchedNcaaGames(validGames, teamsData);
        const filteredTeamGames = _processFilteredTeamGames(filteredGames, teamsData);

        // Write reports and show statistics
        _writeReportsAndStats(unmatchedEspnGames, unmatchedNcaaGames, filteredTeamGames, potentialBindings, verbose);
    } catch (error) {
        if (verbose) console.log(`\u001b[33mWarning: Failed to save reports: ${error.message}\u001b[0m`);
    }
};

/**
 * Generates unique game ID for ESPN games
 * @param {string} espnId - ESPN game ID
 * @param {string} shortTitle - Game short title
 * @returns {string|null} 8-character hash or null if invalid
 */
const generateGameId = (espnId, shortTitle) => {
    if (!espnId) return null;
    return crypto.createHash('md5')
        .update(`CF${espnId}-${shortTitle || ''}`)
        .digest('hex')
        .substring(0, 8)
        .toUpperCase();
};

/**
 * Generates unique game ID for NCAA games
 * @param {string} ncaaGameId - NCAA game ID
 * @param {string} date - Game date
 * @returns {string|null} 8-character hash or null if invalid
 */
const generateNcaaGameId = (ncaaGameId, date) => {
    if (!ncaaGameId) return null;
    return crypto.createHash('md5')
        .update(`NCAA${ncaaGameId}-${date || ''}`)
        .digest('hex')
        .substring(0, 8)
        .toUpperCase();
};

/**
 * Infers venue from team's other home games
 * @param {string} homeTeamId - Home team ID
 * @param {Array<Object>} allEspnGames - All ESPN games data
 * @returns {string|null} Most common venue or null
 */
const _inferVenueFromHomeGames = (homeTeamId, allEspnGames) => {
    if (!homeTeamId || !allEspnGames) return null;

    const homeVenues = allEspnGames
        .filter(game => game.home_id === homeTeamId && game.venue)
        .map(game => game.venue);

    if (homeVenues.length === 0) return null;

    // Find most common venue
    const venueCounts = homeVenues.reduce((counts, venue) => {
        counts[venue] = (counts[venue] || 0) + 1;
        return counts;
    }, {});

    return Object.entries(venueCounts)
        .sort((a, b) => b[1] - a[1])[0][0];
};

/**
 * Infer week for NCAA games by analyzing ESPN games from the same time period
 * @param {Date} gameDate - Game date to analyze
 * @param {number} season - Season year
 * @param {Array} allEspnGames - All ESPN games with week data
 * @returns {number|null} Inferred week number or null
 */
const _inferWeekFromEspnGames = (gameDate, season, allEspnGames) => {
    if (!gameDate || !season || !allEspnGames) return null;

    // Find ESPN games within 3 days of this game date in the same season
    const gameTime = gameDate.getTime();
    const threeDaysMs = 3 * 24 * 60 * 60 * 1000;

    const nearbyEspnGames = allEspnGames.filter(espnGame => {
        if (espnGame.season !== season || !espnGame.week || !espnGame.date_time) return false;

        const espnGameTime = new Date(espnGame.date_time).getTime();
        const timeDiff = Math.abs(gameTime - espnGameTime);

        return timeDiff <= threeDaysMs;
    });

    // Fall back to date-based inference if no nearby ESPN games
    if (nearbyEspnGames.length === 0) return _inferWeekFromDate(gameDate, season);

    // Use the most common week from nearby ESPN games
    const weekCounts = {};
    nearbyEspnGames.forEach(game => {
        weekCounts[game.week] = (weekCounts[game.week] || 0) + 1;
    });

    const mostCommonWeek = Object.entries(weekCounts).sort((a, b) => b[1] - a[1])[0][0];
    return parseInt(mostCommonWeek);
};

/**
 * Generate game objects from NCAA data
 * @param {Object} ncaaCache - NCAA schedules cache
 * @param {Array} teamsData - Teams data
 * @param {Array<number>} yearsToScrape - Years to include
 * @param {Array} allEspnGames - All ESPN games for venue and week inference
 * @param {boolean} verbose - Whether to log progress
 * @returns {Array} Array of NCAA game objects
 */
const _generateNcaaGameObjects = (ncaaCache, teamsData, yearsToScrape, allEspnGames = []) => {
    const ncaaGames = [];
    const processedGameIds = new Set();

    Object.entries(ncaaCache).forEach(([teamNcaaId, teamCache]) => {
        if (!teamCache?.games) return;

        // Get canonical ID for the home team
        const canonicalHomeId = getCanonicalNcaaId(teamNcaaId, 'football');
        const homeTeam = teamsData.find(t => t.reference_id === canonicalHomeId);
        // Continue even if home team not found - we'll create incomplete games for research

        teamCache.games
            .filter(game => yearsToScrape.includes(game.season) && game.ncaa_game_id)
            .forEach(game => {
                if (processedGameIds.has(game.ncaa_game_id)) return;
                processedGameIds.add(game.ncaa_game_id);

                // Get canonical ID for the opponent team
                const canonicalOpponentId = game.opponent_ncaa_id ? getCanonicalNcaaId(game.opponent_ncaa_id, 'football') : null;
                const opponentTeam = canonicalOpponentId ? teamsData.find(t => t.reference_id === canonicalOpponentId) : null;

                // Determine if this is an away game (@ symbol indicates scraped team is going away)
                const isAwayGame = game.opponent_name && game.opponent_name.trim().startsWith('@');

                // Assign home and away teams based on @ symbol
                let actualHomeTeam, actualAwayTeam, actualHomeTeamId, actualAwayTeamId;

                if (isAwayGame) {
                    // Scraped team is going away, opponent is home
                    actualHomeTeam = opponentTeam;
                    actualAwayTeam = homeTeam;
                    actualHomeTeamId = opponentTeam?.id;
                    actualAwayTeamId = homeTeam?.id;
                } else {
                    // Scraped team is home, opponent is away (default behavior)
                    actualHomeTeam = homeTeam;
                    actualAwayTeam = opponentTeam;
                    actualHomeTeamId = homeTeam?.id;
                    actualAwayTeamId = opponentTeam?.id;
                }

                // Continue to create incomplete games for research even if teams not found
                const gameDate = _parseNcaaDate(game.date);
                // Infer venue from other home games for the actual home team
                const inferredVenue = actualHomeTeam ? _inferVenueFromHomeGames(actualHomeTeam.id, allEspnGames) :
                                     (isAwayGame ? 'Unknown' : (homeTeam ? _inferVenueFromHomeGames(homeTeam.id, allEspnGames) : null));
                // Infer week from nearby ESPN games or date-based logic
                const inferredWeek = gameDate ? _inferWeekFromEspnGames(gameDate, game.season, allEspnGames) : null;
                // Parse score to get winner and individual scores (using the scraped team's perspective)
                const scoreData = _parseNcaaScore(game.score, actualHomeTeamId, actualAwayTeamId, homeTeam?.id);
                // Clean up opponent name
                const cleanOpponentName = _cleanNcaaOpponentName(game.opponent_name);

                ncaaGames.push({
                    id: generateNcaaGameId(game.ncaa_game_id, game.date),
                    espn_id: null,
                    reference_id: game.ncaa_game_id,
                    date_time: gameDate?.toISOString() || null,
                    season: game.season,
                    week: inferredWeek,
                    title: actualAwayTeam && actualHomeTeam ? `${actualAwayTeam.full_name} at ${actualHomeTeam.full_name}` :
                           actualHomeTeam ? `${actualAwayTeam?.short_name || cleanOpponentName} at ${actualHomeTeam.full_name}` :
                           actualAwayTeam ? `${actualAwayTeam.full_name} at ${actualHomeTeam?.short_name || 'Unknown'}` :
                           `${cleanOpponentName} at Unknown`,
                    short_title: (() => {
                        // Generate abbreviations for teams without database entries
                        const homeAbv = actualHomeTeam?.abv || _generateTeamAbbreviation(actualHomeTeam?.short_name || 'Unknown');
                        const awayAbv = actualAwayTeam?.abv || _generateTeamAbbreviation(actualAwayTeam?.short_name || cleanOpponentName);
                        return `${awayAbv} @ ${homeAbv}`;
                    })(),
                    venue: inferredVenue,
                    home_espn_id: actualHomeTeam?.espn_id || null,
                    away_espn_id: actualAwayTeam?.espn_id || null,
                    home_score: scoreData.home_score,
                    away_score: scoreData.away_score,
                    winner: scoreData.winner_id,
                    internal_team_id: actualHomeTeam?.id || null,
                    home_id: actualHomeTeam?.id || null,
                    home: actualHomeTeam?.short_name || 'Unknown',
                    home_team_name: actualHomeTeam?.short_name || 'Unknown',
                    away_id: actualAwayTeam?.id || null,
                    away: actualAwayTeam?.short_name || cleanOpponentName,
                    away_team_name: actualAwayTeam?.short_name || cleanOpponentName
                });
            });
    });

    return ncaaGames;
};

/**
 * Match ESPN games with NCAA games for a specific team
 * @param {Array} espnGames - ESPN games for the team
 * @param {Array} ncaaGames - NCAA games for the team  
 * @param {Object} team - Team data object
 * @param {boolean} verbose - Whether to log progress
 * @param {ProgressBar} [progressBar] - Optional progress bar to update
 * @returns {Array} ESPN games with NCAA references added where matches found
 */
const _matchTeamGames = (espnGames, ncaaGames, team, progressBar = null) => {
    if (!espnGames.length || !ncaaGames.length) {
        if (progressBar) progressBar.tick(1, { task: 'Team-by-Team Matching', status: `${team?.short_name || 'Unknown'} - no data` });
        return espnGames;
    }
    
    const teamName = team?.short_name || `Team ${team?.id}`;
    let matchCount = 0;
    const matchedNcaaGameIds = new Set();
    
    const ncaaGamesByDate = new Map();
    ncaaGames.forEach(ncaaGame => {
        if (!ncaaGame.ncaa_game_id || ncaaGame.internal_team_id !== team.id) return;
        const ncaaDate = _parseNcaaDate(ncaaGame.date);
        if (!ncaaDate) return;
        const dateKey = ncaaDate.toISOString().split('T')[0];
        if (!ncaaGamesByDate.has(dateKey)) ncaaGamesByDate.set(dateKey, []);
        ncaaGamesByDate.get(dateKey).push(ncaaGame);
    });
    
    espnGames.forEach(espnGame => {
        if (espnGame.reference_id || espnGame.internal_team_id !== team.id) return;
        const espnDate = new Date(espnGame.date_time);
        if (isNaN(espnDate.getTime())) return;
        const espnDateKey = espnDate.toISOString().split('T')[0];
        const sameDayNcaaGames = ncaaGamesByDate.get(espnDateKey);

        if (sameDayNcaaGames?.length > 0) {
            const availableGame = sameDayNcaaGames.find(ncaaGame => !matchedNcaaGameIds.has(ncaaGame.ncaa_game_id) && ncaaGame.internal_team_id === team.id);
            if (availableGame) {
                espnGame.reference_id = availableGame.ncaa_game_id;
                matchedNcaaGameIds.add(availableGame.ncaa_game_id);
                matchCount++;
            }
        }
    });
    
    if (progressBar) progressBar.tick(1, { task: 'Team-by-Team Matching', status: `${teamName}` });
    return espnGames;
};

/**
 * Removes duplicate games using ESPN ID or reference ID as key
 * @param {Array<Object>} allGames - Array of games to deduplicate
 * @returns {Array<Object>} Unique games sorted by date
 */
const _deduplicateGames = (allGames) => {
    const uniqueGames = allGames.reduce((games, game) => {
        const key = game.espn_id || game.reference_id || game.id;
        if (key) games[key] = game;
        return games;
    }, {});
    return Object.values(uniqueGames).sort((a, b) => new Date(a.date_time || 0) - new Date(b.date_time || 0));
};

/**
 * Initializes ESPN cache analysis and returns statistics
 * @param {Array} teamsData - Teams data
 * @param {Array} yearsToScrape - Years to scrape
 * @param {Object} espnCache - ESPN cache data
 * @param {boolean} verbose - Whether to log progress
 * @returns {Object} Cache statistics
 */
const _analyzeEspnCache = (teamsData, yearsToScrape, espnCache, verbose) => {
    const uniqueEspnIds = [...new Set(teamsData.flatMap(team => team.espn_id || []))];
    const espnCacheDate = espnCache.savedAt ? new Date(espnCache.savedAt).toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit', hour12: true }) : null;
    const [espnCachedCount, espnNewCount] = uniqueEspnIds.reduce((acc, espnId) => {
        acc[_isTeamCacheValid(espnCache.data[espnId], yearsToScrape).isValid ? 0 : 1]++;
        return acc;
    }, [0, 0]);
    if (verbose && espnNewCount > 0) {
        console.log(`\u001b[32mProcessing ESPN games for ${uniqueEspnIds.length} teams across ${yearsToScrape.length} seasons...\u001b[0m`);
        console.log(`\u001b[36mUsing cached ESPN data: ${espnCachedCount} teams, downloading new data: ${espnNewCount} teams\u001b[0m`);
    }
    return { uniqueEspnIds, espnCacheDate, espnCachedCount, espnNewCount };
};

/**
 * Analyzes NCAA cache and returns statistics
 * @param {Array} teamsData - Teams data
 * @param {Array} yearsToScrape - Years to scrape
 * @param {Object} ncaaCache - NCAA cache data
 * @param {boolean} verbose - Whether to log progress
 * @returns {Object} Cache statistics
 */
const _analyzeNcaaCache = (teamsData, yearsToScrape, ncaaCache, verbose) => {
    const uniqueNcaaIds = [...new Set(teamsData.filter(team => team.reference_id).map(team => team.reference_id))];
    let ncaaCachedCount = 0;
    let ncaaNewCount = 0;
    let cacheAnalysisProgress = null;

    if (verbose && uniqueNcaaIds.length > 0) {
        process.stdout.write('\u001b[1A');
        cacheAnalysisProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: uniqueNcaaIds.length, width: 40, complete: '=', incomplete: '-', renderThrottle: 100, clear: false, task: 'NCAA Cache Analysis'
        });
    }

    for (const ncaaId of uniqueNcaaIds) {
        const teamNcaaData = getCacheEntryByCanonicalId(ncaaCache.data, ncaaId, 'football');
        const cacheStatus = _isTeamCacheValid(teamNcaaData, yearsToScrape);
        const team = teamsData.find(t => t.reference_id === ncaaId);
        const teamName = team?.short_name || `NCAA ${ncaaId}`;

        if (cacheStatus.isValid) {
            ncaaCachedCount++;
            if (cacheAnalysisProgress) cacheAnalysisProgress.tick(1, { task: 'NCAA Cache Analysis', status: `${teamName}` });
        } else {
            ncaaNewCount++;
            if (cacheAnalysisProgress) cacheAnalysisProgress.tick(1, { task: 'NCAA Cache Analysis', status: `${teamName} (needs update)` });
        }
    }

    if (cacheAnalysisProgress) cacheAnalysisProgress.terminate();
    return { uniqueNcaaIds, ncaaCachedCount, ncaaNewCount };
};

/**
 * Sets up browsers for NCAA scraping
 * @param {number} ncaaNewCount - Number of teams needing updates
 * @param {boolean} verbose - Whether to log progress
 * @returns {Array} Array of browser instances
 */
const _setupBrowsers = async (ncaaNewCount, verbose) => {
    if (ncaaNewCount === 0) return [];

    const browserCount = Math.min(ncaaNewCount, 3);
    let browsers = [];
    let browserProgress = null;

    if (verbose && browserCount > 1) {
        process.stdout.write('\u001b[1A');
        browserProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total browsers \u001b[90m(:elapseds)\u001b[0m', {
            total: browserCount, width: 40, complete: '=', incomplete: '-', renderThrottle: 100, clear: false, task: 'Browser Setup'
        });
    }

    try {
        for (let i = 0; i < browserCount; i++) {
            try {
                browsers.push(await puppeteer.launch({ headless: true }));
                if (browserProgress) browserProgress.tick(1, { task: 'Browser Setup', status: `Browser ${i + 1} launched` });
            } catch (error) {
                if (verbose) console.log(`\u001b[33mWarning: Failed to launch browser ${i + 1}: ${error.message}\u001b[0m`);
            }
        }
        if (browserProgress) browserProgress.terminate();
    } catch (error) {
        if (verbose) console.log(`\u001b[33mWarning: Failed during browser setup: ${error.message}\u001b[0m`);
    }

    return browsers;
};

/**
 * Processes ESPN games for all teams
 * @param {Array} uniqueEspnIds - Unique ESPN team IDs
 * @param {Array} teamsData - Teams data
 * @param {Array} yearsToScrape - Years to scrape
 * @param {Object} espnCache - ESPN cache data
 * @param {string} taskName - Progress bar task name
 * @param {boolean} verbose - Whether to log progress
 * @returns {Array} All ESPN games
 */
const _processAllEspnGames = async (uniqueEspnIds, teamsData, yearsToScrape, espnCache, taskName, verbose) => {
    let allEspnGames = [];
    let espnProgress = null;

    if (verbose && uniqueEspnIds.length > 0) {
        espnProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: uniqueEspnIds.length, width: 40, complete: '=', incomplete: '-', renderThrottle: 100, clear: false, task: taskName
        });
    }

    for (let i = 0; i < uniqueEspnIds.length; i++) {
        const espnId = uniqueEspnIds[i];
        const team = teamsData.find(t => t.espn_id === espnId);
        const teamName = team?.short_name || `Team ${espnId}`;
        const teamData = espnCache.data[espnId];
        const cacheStatus = _isTeamCacheValid(teamData, yearsToScrape);

        let teamGames;
        if (cacheStatus.isValid) {
            teamGames = teamData.games.filter(game => yearsToScrape.includes(game.season)).map(game => ({ ...game, internal_team_id: team.id }));
            if (espnProgress) espnProgress.tick(1, { task: taskName, status: `${teamName}` });
        } else {
            teamGames = await _processEspnTeamGames(espnId, team, yearsToScrape, espnCache.data, false, espnProgress, null);
            teamGames = teamGames.map(game => ({ ...game, internal_team_id: team.id }));
        }
        allEspnGames.push(...teamGames);
    }

    if (espnProgress) espnProgress.terminate();
    return allEspnGames;
};

/**
 * Retrieves and processes college football games from ESPN and NCAA sources
 * @param {boolean} verbose - Whether to display progress messages
 * @param {boolean} save - Whether to save processed data
 * @returns {Promise<Array<Object>>} Processed game data with team assignments and scores
 */
async function get_formatted_games(verbose = true, save = true) {
    const YEAR_COUNT = 5;
    const currentYear = new Date().getFullYear();
    const yearsToScrape = Array.from({ length: YEAR_COUNT }, (_, i) => currentYear - i);

    const espnCache = cacheManager.get("football_college_espn_schedules", BASE_CACHE_TTL) || { data: {} };
    const ncaaCache = cacheManager.get("football_college_ncaa_schedules", BASE_CACHE_TTL) || { data: {} };
    let initialTeamsData = await get_formated_teams(verbose, save);

    // Analyze and process ESPN cache
    const { uniqueEspnIds, espnCacheDate, espnCachedCount, espnNewCount } = _analyzeEspnCache(initialTeamsData, yearsToScrape, espnCache, verbose);

    const taskName = espnNewCount > 0 ? 'ESPN Data Download' : (espnCacheDate ? `ESPN Data (cached ${espnCacheDate})` : 'ESPN Data (cached)');
    const allEspnGames = await _processAllEspnGames(uniqueEspnIds, initialTeamsData, yearsToScrape, espnCache, taskName, verbose);

    cacheManager.set("football_college_espn_schedules", espnCache.data);
    
    const matchableTeams = initialTeamsData.filter(team => team.espn_id && team.reference_id);

    // Analyze NCAA cache
    const { uniqueNcaaIds, ncaaCachedCount, ncaaNewCount } = _analyzeNcaaCache(initialTeamsData, yearsToScrape, ncaaCache, verbose);

    // Setup browsers for NCAA scraping
    const browsers = await _setupBrowsers(ncaaNewCount, verbose);
    let pages = [];

    if (browsers.length > 0) {
        let pageProgress = null;
        if (verbose && browsers.length > 1) {
            process.stdout.write('\u001b[1A');
            pageProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
                total: browsers.length, width: 40, complete: '=', incomplete: '-', renderThrottle: 100, clear: false, task: 'Page Configuration'
            });
        }

        for (let i = 0; i < browsers.length; i++) {
            try {
                const page = await browsers[i].newPage();
                const { userAgent, headers } = getBrowserConfigWithHeaders({ 'Referer': 'https://stats.ncaa.org/', 'Sec-Fetch-Dest': 'document', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'same-origin', 'Sec-GPC': '1' });
                await Promise.all([
                    page.setUserAgent(userAgent),
                    page.setViewport({ width: 1920, height: 1080 }),
                    page.setExtraHTTPHeaders(headers)
                ]);
                await page.goto('https://stats.ncaa.org/', { waitUntil: 'networkidle2' });
                pages.push(page);
                if (pageProgress) pageProgress.tick(1, { task: 'Page Configuration', status: `Page ${i + 1} configured` });
            } catch (error) {
                if (verbose) console.log(`\u001b[31mFailed to configure page ${i + 1}: ${error.message}\u001b[0m`);
            }
        }
        if (pageProgress) pageProgress.terminate();
    }

    if (ncaaNewCount > 0) await _processNcaaSchedules(uniqueNcaaIds, initialTeamsData, ncaaCache.data, yearsToScrape, pages, verbose);

    let allGames = [...allEspnGames];
    allGames = _deduplicateGames(allGames);
    
    let matchProgress = null;
    if (verbose && matchableTeams.length > 0) {
        process.stdout.write('\u001b[1A');
        matchProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: matchableTeams.length, width: 40, complete: '=', incomplete: '-', renderThrottle: 100, clear: false, task: 'Team-by-Team Matching'
        });
    }
    
    if (matchableTeams.length > 0) {
        for (const team of matchableTeams) {
            const teamEspnGames = allGames.filter(game => game.internal_team_id === team.id);
            const teamCacheData = getCacheEntryByCanonicalId(ncaaCache.data, team.reference_id, 'football');
            const ncaaGames = (teamCacheData?.games?.filter(game => yearsToScrape.includes(game.season)) || []).map(game => ({ ...game, internal_team_id: team.id }));
            _matchTeamGames(teamEspnGames, ncaaGames, team, matchProgress);
        }
        if (matchProgress) matchProgress.terminate();
    }

    const unmatchedGames = allGames.filter(g => !g.reference_id);
    if (verbose && unmatchedGames.length > 0) {
        process.stdout.write('\u001b[1A');
        let finalProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: 2, width: 40, complete: '=', incomplete: '-', renderThrottle: 100, clear: false, task: 'Final Processing'
        });
        finalProgress.tick(1, { task: 'Final Processing', status: `Deduplicating ${allGames.length} games` });
        allGames = await _matchEspnWithNcaaGames(allGames, ncaaCache.data, initialTeamsData, yearsToScrape, false);
        const finalMatchedCount = allGames.filter(g => g.reference_id).length;
        const finalUnmatchedCount = allGames.length - finalMatchedCount;
        finalProgress.tick(1, { task: 'Final Processing', status: `${finalMatchedCount} matched, ${finalUnmatchedCount} unmatched games` });
        finalProgress.terminate();
    } else if (verbose) {
        process.stdout.write('\u001b[1A');
        let finalProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: 1, width: 40, complete: '=', incomplete: '-', renderThrottle: 100, clear: false, task: 'Final Processing'
        });
        const finalMatchedCount = allGames.filter(g => g.reference_id).length;
        const finalUnmatchedCount = allGames.length - finalMatchedCount;
        finalProgress.tick(1, { task: 'Final Processing', status: `${finalMatchedCount} matched, ${finalUnmatchedCount} unmatched games` });
        finalProgress.terminate();
    }

    const potentialBindings = deducePotentialBindings(allGames, ncaaCache.data, initialTeamsData, yearsToScrape);
    const researchableBindings = potentialBindings.filter(b => {
        // ESPN-to-NCAA bindings: require not already bound and confidence >= 2
        if (b.type === 'espn_to_ncaa') {
            return !b.already_bound && b.confidence >= 2;
        }
        // NCAA consolidation bindings: require confidence >= 1 (at least one evidence game)
        if (b.type === 'ncaa_consolidation') {
            return b.confidence >= 1;
        }
        // Legacy bindings (backward compatibility)
        return !b.already_bound && (b.confidence === 'high' || b.confidence === 'medium' || b.confidence >= 2);
    });
    if (researchableBindings.length > 0) {
        process.stdout.write('\u001b[1A');
        if (verbose) console.log(`\u001b[36mFound ${researchableBindings.length} potential bindings to research...\u001b[0m`);
        
        if (browsers.length > 0 && pages.length > 0) {
            try {
                console.log(`\u001b[35mCalling research function with ${researchableBindings.length} researchable bindings...\u001b[0m`);
                const { researchPotentialNewBindings } = await import('../teams/football-teams-college.js');
                await researchPotentialNewBindings(researchableBindings, verbose, browsers);
            } catch (error) { 
                if (verbose) console.log(`\u001b[33mWarning: Failed to research bindings: ${error.message}\u001b[0m`); 
            }
        } else {
            if (verbose) console.log(`\u001b[36mInitializing browsers for binding research...\u001b[0m`);
            try {
                const browserCount = Math.min(3, researchableBindings.length);
                const researchBrowsers = [];
                for (let i = 0; i < browserCount; i++) {
                    try {
                        researchBrowsers.push(await puppeteer.launch({ headless: true }));
                    } catch (error) {
                        if (verbose) console.log(`\u001b[31mFailed to launch research browser ${i + 1}: ${error.message}\u001b[0m`);
                    }
                }
                if (researchBrowsers.length > 0) {
                    if (verbose) console.log(`\u001b[35mCalling research function with ${researchableBindings.length} researchable bindings...\u001b[0m`);
                    const { researchPotentialNewBindings } = await import('../teams/football-teams-college.js');
                    await researchPotentialNewBindings(researchableBindings, verbose, researchBrowsers);
                    await Promise.all(researchBrowsers.map(b => b.close()));
                } else {
                    if (verbose) console.log(`\u001b[31mCould not initialize browsers for binding research.\u001b[0m`);
                }
            } catch (error) {
                if (verbose) console.log(`\u001b[33mWarning: Failed to research bindings: ${error.message}\u001b[0m`);
            }
        }
    }
    if (browsers.length > 0) await Promise.all(browsers.map(b => b.close()));
    process.stdout.write('\u001b[1A');

    // Generate NCAA-only games that weren't matched with ESPN
    const ncaaGames = _generateNcaaGameObjects(ncaaCache.data, initialTeamsData, yearsToScrape, allGames);
    const existingEspnGameIds = new Set(allGames.filter(g => g.reference_id).map(g => g.reference_id));
    const uniqueNcaaGames = ncaaGames.filter(g => g.reference_id && !existingEspnGameIds.has(g.reference_id));


    if (verbose) console.log(`\u001b[36mCombining ${allGames.length} ESPN games with ${uniqueNcaaGames.length} NCAA-only games...\u001b[0m`);
    const allCombinedGames = [...allGames, ...uniqueNcaaGames];

    const finalGames = _deduplicateGames(allCombinedGames);
    finalGames.forEach(game => {
        if (!game.id) {
            game.id = game.espn_id ? generateGameId(game.espn_id, game.short_title) : generateNcaaGameId(game.reference_id, game.date_time);
        }
    });


    // Transform to required schema format
    const transformedGames = finalGames.map(game => {
        const gameDate = new Date(game.date_time);

        // Format time as 12-hour format with AM/PM
        const formatTime = (date) => {
            if (!date || isNaN(date.getTime())) return null;
            return date.toLocaleTimeString('en-US', {
                hour: 'numeric',
                minute: '2-digit',
                hour12: true
            });
        };

        const weekNumber = game.week || _inferWeekFromDate(gameDate, game.season);
        const transformedGame = {
            id: game.id,
            espn_id: game.espn_id,
            reference_id: game.reference_id || null,
            league_id: 'NCAAF',
            week: weekNumber,
            date_time: game.date_time,
            date: gameDate.toISOString().split('T')[0],
            time: formatTime(gameDate),
            season_type: weekNumber ? 'REG' : 'POST',
            season: game.season,
            title: game.title,
            short_title: game.short_title,
            venue: game.venue,
            home: game.home || game.home_team_name,
            home_id: game.home_id,
            away: game.away || game.away_team_name,
            away_id: game.away_id,
            winner_id: game.winner
        };
        return transformedGame;
    });

    // Infer missing team IDs using cross-schedule matching
    const _inferMissingTeamIds = (games) => {
        // Group games by reference_id for quick lookup
        const gamesByRefId = {};

        // First pass: index all games by reference_id
        games.forEach(game => {
            if (game.reference_id) {
                if (!gamesByRefId[game.reference_id]) gamesByRefId[game.reference_id] = [];
                gamesByRefId[game.reference_id].push(game);
            }
        });

        let inferredCount = 0;

        // Second pass: infer missing team IDs
        games.forEach(game => {
            const missingHome = !game.home_id || game.home_id === 'Unknown';
            const missingAway = !game.away_id || game.away_id === 'Unknown';

            if ((missingHome || missingAway) && game.reference_id && gamesByRefId[game.reference_id]) {
                const matchingGames = gamesByRefId[game.reference_id].filter(g => g !== game);

                for (const otherGame of matchingGames) {
                    // If we have a complete matching game, use its team IDs
                    if (otherGame.home_id && otherGame.away_id &&
                        otherGame.home_id !== 'Unknown' && otherGame.away_id !== 'Unknown') {

                        if (missingHome && !missingAway) {
                            // We know away team, infer home team
                            if (otherGame.away_id === game.away_id) {
                                game.home_id = otherGame.home_id;
                                game.home = otherGame.home;
                                inferredCount++;
                            } else if (otherGame.home_id === game.away_id) {
                                game.home_id = otherGame.away_id;
                                game.home = otherGame.away;
                                inferredCount++;
                            }
                        } else if (!missingHome && missingAway) {
                            // We know home team, infer away team
                            if (otherGame.home_id === game.home_id) {
                                game.away_id = otherGame.away_id;
                                game.away = otherGame.away;
                                inferredCount++;
                            } else if (otherGame.away_id === game.home_id) {
                                game.away_id = otherGame.home_id;
                                game.away = otherGame.home;
                                inferredCount++;
                            }
                        } else if (missingHome && missingAway) {
                            // Missing both, copy from complete game (this shouldn't happen often)
                            game.home_id = otherGame.home_id;
                            game.home = otherGame.home;
                            game.away_id = otherGame.away_id;
                            game.away = otherGame.away;
                            inferredCount++;
                        }
                        break;
                    }
                }
            }
        });
        if (verbose && inferredCount > 0) console.log(`\u001b[32mInferred ${inferredCount} missing team IDs using cross-schedule matching\u001b[0m`);
        return games;
    };

    const gamesWithInferredIds = _inferMissingTeamIds(transformedGames);

    /**
     * Create an incomplete game object using available data and inference
     * @param {Object} game - Original game object
     * @param {Array} allEspnGames - All ESPN games for inference
     * @param {Array} teamsData - Teams data for lookup
     * @returns {Object} Incomplete game object with inferred data where possible
     */
    const _createIncompleteGame = (game, allEspnGames, teamsData) => {
        const gameDate = new Date(game.date_time);
        const weekNumber = game.week || _inferWeekFromDate(gameDate, game.season);

        // Format time as 12-hour format with AM/PM
        const formatTime = (date) => {
            if (!date || isNaN(date.getTime())) return null;
            return date.toLocaleTimeString('en-US', {
                hour: 'numeric',
                minute: '2-digit',
                hour12: true
            });
        };

        // Determine which team info we have and which is missing
        const hasHomeId = game.home_id && game.home_id !== 'Unknown';
        const hasAwayId = game.away_id && game.away_id !== 'Unknown';

        let homeTeamName = game.home || game.home_team_name || 'Unknown';
        let awayTeamName = game.away || game.away_team_name || 'Unknown';
        let inferredVenue = game.venue;

        // If home team is missing but we have ESPN IDs, try to get team names from ESPN data
        if (!hasHomeId && game.home_espn_id) {
            // Try to find home team name from ESPN games involving this ESPN ID
            const espnGameWithHome = allEspnGames.find(g => g.home_espn_id === game.home_espn_id);
            if (espnGameWithHome) {
                homeTeamName = espnGameWithHome.home_team_name || espnGameWithHome.home || homeTeamName;
            }
        }

        if (!hasAwayId && game.away_espn_id) {
            // Try to find away team name from ESPN games involving this ESPN ID
            const espnGameWithAway = allEspnGames.find(g => g.away_espn_id === game.away_espn_id);
            if (espnGameWithAway) {
                awayTeamName = espnGameWithAway.away_team_name || espnGameWithAway.away || awayTeamName;
            }
        }

        // If home team is missing, venue should default to 'Unknown'
        if (!hasHomeId) {
            inferredVenue = 'Unknown';
        } else if (!inferredVenue && hasHomeId) {
            // Try to infer venue from other home games
            inferredVenue = _inferVenueFromHomeGames(game.home_id, allEspnGames) || 'Unknown';
        }

        // Build title from available information
        let title = game.title;
        if (!title) {
            title = `${awayTeamName} at ${homeTeamName}`;
        }

        let shortTitle = game.short_title;
        if (!shortTitle) {
            // Generate short title using available data
            const homeTeam = hasHomeId ? teamsData.find(t => t.id === game.home_id) : null;
            const awayTeam = hasAwayId ? teamsData.find(t => t.id === game.away_id) : null;

            const homeAbv = homeTeam?.abv || _generateTeamAbbreviation(homeTeamName);
            const awayAbv = awayTeam?.abv || _generateTeamAbbreviation(awayTeamName);

            shortTitle = `${awayAbv} @ ${homeAbv}`;
        }

        return {
            id: game.id,
            espn_id: game.espn_id || null,
            reference_id: game.reference_id || null,
            league_id: 'NCAAF',
            week: weekNumber,
            date_time: game.date_time,
            date: gameDate.toISOString().split('T')[0],
            time: formatTime(gameDate),
            season_type: weekNumber ? 'REG' : 'POST',
            season: game.season,
            title: title,
            short_title: shortTitle,
            venue: inferredVenue,
            home: homeTeamName,
            home_id: hasHomeId ? game.home_id : null,
            away: awayTeamName,
            away_id: hasAwayId ? game.away_id : null,
            winner_id: game.winner_id || game.winner || null
        };
    };

    // Process all games - create complete or incomplete versions
    const allProcessedGames = [];
    const filteredGames = [];

    gamesWithInferredIds.forEach(game => {
        const hasHomeId = game.home_id && game.home_id !== 'Unknown';
        const hasAwayId = game.away_id && game.away_id !== 'Unknown';

        if (hasHomeId && hasAwayId) {
            // Complete game - both teams have IDs
            allProcessedGames.push(game);
        } else {
            // Incomplete game - create using available data
            const incompleteGame = _createIncompleteGame(game, allGames, initialTeamsData);
            allProcessedGames.push(incompleteGame);

            // Still track filtered games for reporting
            filteredGames.push({
                title: game.title,
                home_espn_id: game.home_espn_id || null,
                away_espn_id: game.away_espn_id || null,
                home_id: game.home_id || 'MISSING',
                away_id: game.away_id || 'MISSING',
                source: game.espn_id && game.reference_id ? 'both' : (game.espn_id ? 'ESPN' : 'NCAA')
            });
        }
    });

    const validGames = allProcessedGames;

    if (verbose) {
        const completeGames = allProcessedGames.filter(g => g.home_id && g.away_id).length;
        const incompleteGames = allProcessedGames.length - completeGames;
        console.log(`\u001b[36mProcessed ${allProcessedGames.length} total games: ${completeGames} with both team IDs, ${incompleteGames} with missing team IDs\u001b[0m`);

        // Show breakdown of missing teams
        if (filteredGames.length > 0) {
            const missingHomeTeams = new Set();
            const missingAwayTeams = new Set();

            const ncaaOnlyProblems = [];

            filteredGames.forEach(game => {
                if (game.home_id === 'MISSING' || game.home_id === 'Unknown') {
                    if (game.home_espn_id) {
                        missingHomeTeams.add(game.home_espn_id);
                    } else {
                        ncaaOnlyProblems.push(`Home team missing in: "${game.title}"`);
                    }
                }
                if (game.away_id === 'MISSING' || game.away_id === 'Unknown') {
                    if (game.away_espn_id) {
                        missingAwayTeams.add(game.away_espn_id);
                    } else {
                        ncaaOnlyProblems.push(`Away team missing in: "${game.title}"`);
                    }
                }
            });

        }
    }

    if (save) {
        const finalPotentialBindings = deducePotentialBindings(finalGames, ncaaCache.data, initialTeamsData, yearsToScrape);
        _saveUnmatchedReports(validGames, finalGames, initialTeamsData, finalPotentialBindings, verbose);
        fs.writeFileSync(OUTPUT_FILE, JSON.stringify(validGames, null, 2), "utf8");
        if (verbose) console.log(`\u001b[90mCollege Football Games Data Saved To: ${OUTPUT_FILE}\u001b[0m`);
    }
    return validGames;
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
    get_formatted_games()
}