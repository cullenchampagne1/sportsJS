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

// File to hold formatted data
const OUTPUT_FILE = "data/processed/football-games-college.json";
// Base cache TTL (30 days for global cache)
const BASE_CACHE_TTL = 30 * 24 * 60 * 60 * 1000;
// Browser colors for console output
const BROWSER_COLORS = ['\u001b[33m', '\u001b[34m', '\u001b[32m'];

/**
 * Calculate absolute expiration time for a team based on their next upcoming game
 * @param {Array<Object>} games - Array of games for the team
 * @returns {number} Absolute expiration time in milliseconds (1 hour after next game, minimum 30 days from now)
 */
const _calculateExpirationTime = (games) => {
    if (!games || games.length === 0) return Date.now() + BASE_CACHE_TTL;
    const now = Date.now();
    const upcomingGames = games
        .filter(game => game.date_time)
        .map(game => new Date(game.date_time))
        .filter(gameDate => gameDate.getTime() > now)
        .sort((a, b) => a.getTime() - b.getTime());
    if (upcomingGames.length > 0) {
        const nextGame = upcomingGames[0];
        return nextGame.getTime() + (60 * 60 * 1000);
    }
    return now + BASE_CACHE_TTL;
};

/**
 * Check if cached team data has expired and covers all required years.
 * @param {Object} teamData - Team data with games, savedAt timestamp, and expiresAt.
 * @param {Array<number>} requiredYears - Years that must be present in the cache.
 * @returns {Object} { isValid: boolean, missingYears: Array<number>, needsCurrentYearOnly: boolean }.
 */
const _isTeamCacheValid = (teamData, requiredYears) => {
    // If no cache exists, it's invalid
    if (!teamData || !teamData.savedAt) {
        return {
            isValid: false,
            missingYears: requiredYears,
            needsCurrentYearOnly: false
        };
    }
    
    // If cache has expired, need refresh
    if (Date.now() >= teamData.expiresAt) {
        const cachedYears = teamData.games ? new Set(teamData.games.map(g => g.season)) : new Set();
        const missingYears = requiredYears.filter(year => !cachedYears.has(year));
        const needsCurrentYearOnly = teamData.games?.length > 0;
        return {
            isValid: false,
            missingYears: needsCurrentYearOnly ? [Math.max(...requiredYears)] : missingYears,
            needsCurrentYearOnly
        };
    }
    
    // Cache is fresh - check if it covers all required years
    // Note: Empty games array is valid if explicitly cached (team genuinely has no games)
    const cachedYears = new Set((teamData.fetchedYears || teamData.games?.map(g => g.season)) || []);
    const missingYears = requiredYears.filter(year => !cachedYears.has(year));
    return {
        isValid: missingYears.length === 0,
        missingYears,
        needsCurrentYearOnly: false
    };
};

/**
 * Fetches ESPN games data for a single team and year.
 * @param {string} espnId - ESPN team ID.
 * @param {number} year - Season year to fetch.
 * @param {boolean} verbose - Whether to log progress.
 * @returns {Array} Array of game objects for the team/year.
 */
const _fetchEspnTeamGames = async (espnId, year, verbose) => {
    verbose && console.log(`\u001b[32mDownloading Schedule: https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/${espnId}/schedule?season=${year}\u001b[0m`);
    const response = await fetch(`https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/${espnId}/schedule?season=${year}`);
    if (!response.ok) return [];
    const scheduleJson = await response.json();
    if (!scheduleJson?.events) return [];
    const games = scheduleJson.events.flatMap(game => {
        const comp = game.competitions?.[0];
        if (!comp || !comp.competitors) return [];
        const [homeComp, awayComp] = [comp.competitors.find(c => c.homeAway === 'home'), comp.competitors.find(c => c.homeAway === 'away')];
        if (!homeComp || !awayComp) return [];
        const [homeScore, awayScore] = [homeComp.score?.value || 0, awayComp.score?.value || 0];
        const winner = homeScore > awayScore ? homeComp.team.id : (awayScore > homeScore ? awayComp.team.id : null);
        return [{
            espn_id: game.id, date_time: game.date, season: year, title: game.name || null, short_title: game.shortName || null,
            venue: comp.venue?.fullName || null, home_espn_id: homeComp.team.id, away_espn_id: awayComp.team.id, home_score: homeScore,
            away_score: awayScore, winner: winner
        }];
    });
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
        if (progressBar) {
            progressBar.tick(1, { 
                status: `${teamName}`
            });
        }
        return teamData.games.filter(game => yearsToScrape.includes(game.season));
    }
    
    let teamGames = (teamData?.games || []).filter(game => yearsToScrape.includes(game.season));
    const yearsToFetch = cacheStatus.missingYears;
    
    if (yearsToFetch.length > 0) {
        // Create progress bar for years if we have multiple years to fetch
        let yearProgress = null;
        if (verbose && yearsToFetch.length > 1) {
            yearProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total years \u001b[90m(:elapseds elapsed)\u001b[0m', {
                total: yearsToFetch.length,
                width: 40,
                complete: '=',
                incomplete: '-',
                renderThrottle: 100,
                clear: false,
                task: `${teamName} - ESPN Years`
            });
        }
        
        teamGames = teamGames.filter(game => !yearsToFetch.includes(game.season));
        let successCount = 0;
        let errorCount = 0;
        
        for (const year of yearsToFetch) {
            try {
                const newGames = await _fetchEspnTeamGames(espnId, year, false); // Suppress individual logs
                teamGames.push(...newGames);
                successCount++;
                
                if (yearProgress) {
                    yearProgress.tick(1, {
                        task: `${teamName} - ESPN Years`,
                        status: `${year} (${newGames.length} games)`
                    });
                }
            } catch (error) {
                errorCount++;
                if (yearProgress) {
                    yearProgress.tick(1, {
                        task: `${teamName} - ESPN Years`,
                        status: `${year} (ERROR: ${error.message})`
                    });
                }
            }
        }
        
        if (yearProgress) yearProgress.terminate();
    }
    
    // Track which years were fetched, even if they returned no games
    const existingFetchedYears = new Set(teamData?.fetchedYears || []);
    yearsToFetch.forEach(year => existingFetchedYears.add(year));
    const allFetchedYears = [...existingFetchedYears, ...(teamData?.games?.map(g => g.season) || [])];
    
    allSchedulesCache[espnId] = {
        games: teamGames,
        savedAt: Date.now(),
        expiresAt: _calculateExpirationTime(teamGames),
        fetchedYears: [...new Set(allFetchedYears)] // Track all years we've attempted to fetch
    };
    cacheManager.set("football_college_espn_schedules", allSchedulesCache);
    
    if (progressBar) {
        progressBar.tick(1, { 
            task: 'ESPN Data Download',
            status: `${teamName} - ${teamGames.length} games (${yearsToFetch.length} years fetched)`
        });
    }
    
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
    const log = (msg, color = BROWSER_COLORS[pageIndex]) => verbose && console.log(`${color}${msg}\u001b[0m`);
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
                    const scheduleTable = Array.from(document.querySelectorAll('table')).find(table => {
                        const header = table.querySelector('tr > td, tr > th');
                        return header && header.textContent.trim().toLowerCase() === 'date';
                    });
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
                
                // Apply consolidation after getting data from browser context
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
    const teamsToProcess = ncaaIds.filter(id => {
        const teamCacheData = getCacheEntryByCanonicalId(cache, id, 'football');
        return !_isTeamCacheValid(teamCacheData, yearsToScrape).isValid;
    });

    if (pages.length === 0) return;
    if (teamsToProcess.length === 0) return;
    
    // Create comprehensive progress bar for NCAA processing
    let ncaaProgress = null;
    if (verbose && teamsToProcess.length > 0) {
        process.stdout.write('\u001b[1A');
        ncaaProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: teamsToProcess.length,
            width: 40,
            complete: '=',
            incomplete: '-',
            renderThrottle: 100,
            clear: false,
            task: 'NCAA Data Scraping'
        });
    }

    let pageIndex = 0;
    let successCount = 0;
    let errorCount = 0;
    
    for (const ncaaId of teamsToProcess) {
        const page = pages[pageIndex % pages.length];
        const team = teamsData.find(t => t.reference_id === ncaaId);
        const teamName = team?.short_name || `NCAA ${ncaaId}`;
        const teamCacheData = getCacheEntryByCanonicalId(cache, ncaaId, 'football');
        const cacheStatus = _isTeamCacheValid(teamCacheData, yearsToScrape);
        const yearsToFetch = cacheStatus.missingYears.length > 0 ? cacheStatus.missingYears : yearsToScrape;
        try {
            const newGames = await _scrapeNcaaTeamSchedule(ncaaId, yearsToFetch, page, false, pageIndex % pages.length); // Suppress individual logs

            const existingGames = (teamCacheData?.games || []).filter(game => !yearsToFetch.includes(game.season));
            const allGames = [...existingGames, ...newGames];

            const gamesWithDateTime = allGames.map(g => ({ date_time: _parseNcaaDate(g.date)?.toISOString() || null }));
            // Track which years were fetched, even if they returned no games
            const existingFetchedYears = new Set(teamCacheData?.fetchedYears || []);
            yearsToFetch.forEach(year => existingFetchedYears.add(year));
            const allGameYears = allGames.map(g => g.season);
            const allFetchedYears = [...existingFetchedYears, ...allGameYears];
            
            cache[ncaaId] = {
                games: allGames,
                savedAt: Date.now(),
                expiresAt: _calculateExpirationTime(gamesWithDateTime),
                fetchedYears: [...new Set(allFetchedYears)] // Track all years we've attempted to fetch
            };

            successCount++;
            if (ncaaProgress) {
                ncaaProgress.tick(1, {
                    task: 'NCAA Data Scraping',
                    status: `${teamName} - ${newGames.length} games`,
                    success: successCount,
                    errors: errorCount
                });
            }
        } catch (error) {
            errorCount++;
            if (ncaaProgress) {
                ncaaProgress.tick(1, {
                    task: 'NCAA Data Scraping',
                    status: `${teamName} - ERROR: ${error.message}`,
                    success: successCount,
                    errors: errorCount
                });
            }
        }

        cacheManager.set("football_college_ncaa_schedules", cache);
        pageIndex++;
        await new Promise(resolve => setTimeout(resolve, 3000));
    }

    if (ncaaProgress) {
        ncaaProgress.terminate();
    }

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
    if (daysDifference > 7) return false; // Keep a reasonable date window
    // Collect available NCAA IDs from teams that exist
    const availableNcaaIds = [];
    if (homeTeam?.reference_id) availableNcaaIds.push(homeTeam.reference_id);
    if (awayTeam?.reference_id) availableNcaaIds.push(awayTeam.reference_id);
    // If no teams have NCAA IDs, cannot match
    if (availableNcaaIds.length === 0) return false;
    const espnTeamNcaaIds = new Set(availableNcaaIds);
    // Removed unused ncaaGameIds variable
    // For same-day matches (< 1.5 days), require only single team match
    if (daysDifference <= 1.5) {
        return espnTeamNcaaIds.has(ncaaGame.home_team_ncaa_id) || espnTeamNcaaIds.has(ncaaGame.opponent_ncaa_id);
    }
    // For matches with larger time differences, require more confidence
    if (availableNcaaIds.length === 2) {
        // Both teams available - require both teams to match
        return espnTeamNcaaIds.has(ncaaGame.home_team_ncaa_id) && espnTeamNcaaIds.has(ncaaGame.opponent_ncaa_id);
    } else {
        // Only one team available - require it to match and no conflicting team
        const hasMatchingTeam = espnTeamNcaaIds.has(ncaaGame.home_team_ncaa_id) || espnTeamNcaaIds.has(ncaaGame.opponent_ncaa_id);
        return hasMatchingTeam;
    }
};

/**
 * Parses an NCAA date string into a valid JavaScript Date object.
 * @param {string | undefined} dateStr - The date string to parse.
 * @returns {Date | null} A Date object if parsing is successful, otherwise null.
 */
const _parseNcaaDate = (dateStr) => {
    if (!dateStr) return null;
    if (dateStr.includes('/')) {
        const [month, day, year] = dateStr.split(' ')[0].split('/').map(Number);
        if ([month, day, year].some(isNaN)) return null;
        const d = new Date(year, month - 1, day);
        return d.getMonth() + 1 === month ? d : null;
    }
    const d = new Date(dateStr);
    return isNaN(d.getTime()) ? null : d;
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
    const log = (msg, color = '\u001b[36m') => verbose && console.log(`${color}${msg}\u001b[0m`);

    // Use teams data as-is since consolidation has already been applied during team processing
    const allNcaaGames = Object.values(ncaaSchedulesCache).flatMap(cache => (cache?.games || []).filter(game => yearsToScrape.includes(game.season)));
    // Use NCAA games as-is since consolidation is already applied during scraping
    const ncaaGames = Object.values(allNcaaGames.reduce((acc, game) => (game.ncaa_game_id && !acc[game.ncaa_game_id]) ? { ...acc, [game.ncaa_game_id]: game } : acc, {}));

    const completedEspnGames = espnGames.filter(game => new Date(game.date_time) < now);
    log(`Matching ${completedEspnGames.length} completed ESPN games with ${ncaaGames.length} NCAA games...`, '\u001b[32m');

    let skippedDuplicateMatches = 0;
    const matchedNcaaGameIds = new Set();

    completedEspnGames.forEach(espnGame => {
        if (espnGame.reference_id) return; // Already matched in a previous iteration

        const espnDate = new Date(espnGame.date_time);
        const title = espnGame.title || espnGame.short_title || '';
        const teams = title.split(/\s(?:vs|at|@)\s/i);
        let homeTeam = teamsData.find(t => t.espn_id === espnGame.home_espn_id) || (teams.length === 2 ? findTeamByName(teams[1], teamsData) : null);
        let awayTeam = teamsData.find(t => t.espn_id === espnGame.away_espn_id) || (teams.length === 2 ? findTeamByName(teams[0], teamsData) : null);

        // Allow matching with partial team data - let _isMatch decide if we have enough info

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

        // Assign team names and IDs using available data, fallback to "Unknown" for missing teams
        if (homeTeam) { 
            game.home_id = homeTeam.id; 
            game.home = homeTeam.short_name;
            game.home_team_name = homeTeam.short_name;
        } else {
            game.home_team_name = "Unknown";
        }
        
        if (awayTeam) { 
            game.away_id = awayTeam.id; 
            game.away = awayTeam.short_name;
            game.away_team_name = awayTeam.short_name;
        } else {
            game.away_team_name = "Unknown";
        }
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
 * Deduces potential ESPN-to-NCAA team bindings from games where one team is matched and the other is not.
 * @param {Array} espnGames - Array of processed ESPN games.
 * @param {Object} ncaaSchedulesCache - The cache of NCAA schedules.
 * @param {Array} teamsData - The current consolidated teams data.
 * @param {Array<number>} yearsToScrape - The years being processed.
 * @returns {Array} An array of potential binding objects.
 */
const deducePotentialBindings = (espnGames, ncaaSchedulesCache, teamsData, yearsToScrape) => {
    const potentialBindingsMap = new Map();
    const seenBindings = new Set();

    const allNcaaGames = Object.values(ncaaSchedulesCache).flatMap(teamCache =>
        (teamCache.games || []).filter(game => yearsToScrape.includes(game.season))
    );

    espnGames.forEach(espnGame => {
        if (!espnGame.date_time || espnGame.reference_id) return; // Skip if already matched

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
            if (!ncaaDate) return false;
            const involvesBoundTeam = ncaaGame.home_team_ncaa_id === boundTeam.reference_id || ncaaGame.opponent_ncaa_id === boundTeam.reference_id;
            if (!involvesBoundTeam) return false;
            const daysDiff = Math.abs((espnDate - ncaaDate) / (1000 * 60 * 60 * 24));
            return daysDiff < 2.5;
        });

        matchingNcaaGames.forEach(ncaaGame => {
            const opponentNcaaId = ncaaGame.home_team_ncaa_id === boundTeam.reference_id
                ? ncaaGame.opponent_ncaa_id
                : ncaaGame.home_team_ncaa_id;

            if (!opponentNcaaId) return;

            const bindingKey = `${unboundEspnId}-${opponentNcaaId}`;
            
            // Aggregate evidence for this potential binding
            const existingBinding = potentialBindingsMap.get(bindingKey) || {
                espn_id: unboundEspnId,
                ncaa_id: opponentNcaaId,
                evidence_games: [],
                confidence: 0,
                bound_team_name: boundTeam.short_name,
                espn_game_title: espnGame.title
            };

            existingBinding.evidence_games.push({ espn_game_id: espnGame.espn_id, ncaa_game_id: ncaaGame.ncaa_game_id });
            existingBinding.confidence = existingBinding.evidence_games.length;

            const existingTeamWithNcaaId = teamsData.find(team => team.reference_id === opponentNcaaId);
            existingBinding.already_bound = !!existingTeamWithNcaaId;
            existingBinding.existing_espn_id = existingTeamWithNcaaId?.espn_id || null;

            potentialBindingsMap.set(bindingKey, existingBinding);
        });
    });

    return Array.from(potentialBindingsMap.values());
};

/**
 * Saves reports of unmatched games and potential new team bindings.
 * @param {Array} matchedGames - The final array of processed games.
 * @param {Object} ncaaSchedulesCache - The cache of NCAA schedules.
 * @param {Array} teamsData - The current consolidated teams data.
 * @param {Array} potentialBindings - Deduced potential bindings.
 * @param {boolean} verbose - Whether to log progress.
 */
const _saveUnmatchedReports = (matchedGames, ncaaSchedulesCache, teamsData, potentialBindings, yearsToScrape, verbose) => {
    try {
        if (!fs.existsSync('output')) fs.mkdirSync('output', { recursive: true });
        if (!fs.existsSync('output/csv')) fs.mkdirSync('output/csv', { recursive: true });

        const now = new Date();
        const unmatchedEspnGames = matchedGames
            .filter(game => new Date(game.date_time) < now && !game.reference_id)
            .map(game => ({
                espn_id: game.espn_id,
                date_time: game.date_time,
                title: game.title,
                home_espn_id: game.home_espn_id,
                away_espn_id: game.away_espn_id,
                home_team: teamsData.find(t => t.espn_id === game.home_espn_id)?.short_name || 'Unknown',
                away_team: teamsData.find(t => t.espn_id === game.away_espn_id)?.short_name || 'Unknown',
            }));

        const allMatchedNcaaIds = new Set(matchedGames.map(g => g.reference_id).filter(Boolean));
        const uniqueNcaaGames = Object.values(Object.values(ncaaSchedulesCache).flatMap(c => (c.games || []).filter(g => yearsToScrape.includes(g.season))).reduce((acc, game) => {
            if (game.ncaa_game_id) acc[game.ncaa_game_id] = game;
            return acc;
        }, {}));

        const unmatchedNcaaGames = uniqueNcaaGames
            .filter(game => !allMatchedNcaaIds.has(game.ncaa_game_id))
            .map(game => ({
                ncaa_game_id: game.ncaa_game_id,
                date: game.date,
                home_team_ncaa_id: game.home_team_ncaa_id,
                opponent_name: game.opponent_name,
                opponent_ncaa_id: game.opponent_ncaa_id,
            }));

        fs.writeFileSync('output/unmatched-espn-football-games.json', JSON.stringify(unmatchedEspnGames, null, 2));
        fs.writeFileSync('output/unmatched-ncaa-football-games.json', JSON.stringify(unmatchedNcaaGames, null, 2));
        verbose && console.log(`\u001b[90mSaved ${unmatchedEspnGames.length} unmatched ESPN games and ${unmatchedNcaaGames.length} unmatched NCAA games.\u001b[0m`);

        if (potentialBindings.length > 0) {
            const csvHeader = 'espn_id,ncaa_id,sport,espn_game_title,bound_team_name,confidence,already_bound,existing_espn_id';
            const csvRows = potentialBindings.map(b => `${b.espn_id},${b.ncaa_id},football,"${b.espn_game_title}","${b.bound_team_name}",${b.confidence},${b.already_bound},${b.existing_espn_id || ''}`);
            fs.writeFileSync('output/csv/new-potential-binds.csv', [csvHeader, ...csvRows].join('\n'), 'utf8');
            verbose && console.log(`\u001b[32mGenerated ${potentialBindings.length} potential bindings in output/csv/new-potential-binds.csv\u001b[0m`);
        }
    } catch (error) {
        verbose && console.log(`\u001b[33mWarning: Failed to save reports: ${error.message}\u001b[0m`);
    }
};

/**
 * Generates a unique game ID using ESPN ID and game title.
 * @param {string} espnId - The ESPN game ID.
 * @param {string} shortTitle - The short title of the game.
 * @returns {string} An 8-character unique identifier.
 */
const generateGameId = (espnId, shortTitle) => {
    if (!espnId) return null;
    const input = `CF${espnId}-${shortTitle || ''}`;
    return crypto.createHash('md5').update(input).digest('hex').substring(0, 8).toUpperCase();
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
const _matchTeamGames = (espnGames, ncaaGames, team, verbose, progressBar = null) => {
    if (!espnGames.length || !ncaaGames.length) {
        if (progressBar) {
            progressBar.tick(1, {
                task: 'Team-by-Team Matching',
                status: `${team?.short_name || 'Unknown'} - no data`,
                success: progressBar.curr,
                errors: 0
            });
        }
        return espnGames;
    }
    
    const teamName = team?.short_name || `Team ${team?.id}`;
    let matchCount = 0;
    const matchedNcaaGameIds = new Set();
    
    // Simple approach: match by same internal team ID + same date
    // Create a date-based lookup for NCAA games (YYYY-MM-DD format)
    const ncaaGamesByDate = new Map();
    ncaaGames.forEach(ncaaGame => {
        if (!ncaaGame.ncaa_game_id || ncaaGame.internal_team_id !== team.id) return;
        
        const ncaaDate = _parseNcaaDate(ncaaGame.date);
        if (!ncaaDate) return;
        
        const dateKey = ncaaDate.toISOString().split('T')[0]; // YYYY-MM-DD
        if (!ncaaGamesByDate.has(dateKey)) {
            ncaaGamesByDate.set(dateKey, []);
        }
        ncaaGamesByDate.get(dateKey).push(ncaaGame);
    });
    
    espnGames.forEach(espnGame => {
        if (espnGame.reference_id || espnGame.internal_team_id !== team.id) return; // Already matched or wrong team
        
        const espnDate = new Date(espnGame.date_time);
        if (isNaN(espnDate.getTime())) return;
        
        const espnDateKey = espnDate.toISOString().split('T')[0]; // YYYY-MM-DD
        
        // Look for NCAA games with same internal team ID on the same date
        const sameDayNcaaGames = ncaaGamesByDate.get(espnDateKey);
        if (sameDayNcaaGames && sameDayNcaaGames.length > 0) {
            // Find first available NCAA game on same day with same internal team ID
            const availableGame = sameDayNcaaGames.find(ncaaGame => 
                !matchedNcaaGameIds.has(ncaaGame.ncaa_game_id) && 
                ncaaGame.internal_team_id === team.id
            );
            
            if (availableGame) {
                // Automatic match - same internal team ID, same date
                espnGame.reference_id = availableGame.ncaa_game_id;
                matchedNcaaGameIds.add(availableGame.ncaa_game_id);
                matchCount++;
            }
        }
    });
    
    if (progressBar) {
        progressBar.tick(1, {
            task: 'Team-by-Team Matching',
            status: `${teamName} - ${matchCount}/${espnGames.length} matched`,
            success: progressBar.curr,
            errors: 0
        });
    }
    
    return espnGames;
};

/**
 * College Football Games
 * Retrieves and processes college football game data from ESPN and NCAA sources.
 * @param {boolean} verbose - Whether to print progress messages (default: true)
 * @param {boolean} save - Whether to save data to data/processed folder
 * @returns {Array} Array containing processed game information.
 */
async function get_formatted_games(verbose = true, save = true) {
    const YEAR_COUNT = 3;
    const currentYear = new Date().getFullYear();
    const yearsToScrape = Array.from({ length: YEAR_COUNT }, (_, i) => currentYear - i);

    // --- Initialize caches and data ---
    const espnCache = cacheManager.get("football_college_espn_schedules", BASE_CACHE_TTL) || { data: {} };
    const ncaaCache = cacheManager.get("football_college_ncaa_schedules", BASE_CACHE_TTL) || { data: {} };
    let initialTeamsData = await get_formated_teams(verbose, save);
    
    // Get unique ESPN IDs from teams data
    const uniqueEspnIds = [...new Set(
        initialTeamsData
            .filter(team => team.espn_id)
            .map(team => team.espn_id)
    )];
    
    // Get cache date for progress bar display
    const espnCacheDate = espnCache.savedAt ? new Date(espnCache.savedAt).toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit', hour12: true }) : null;
    
    // Count cached vs new ESPN teams
    let espnCachedCount = 0;
    let espnNewCount = 0;
    
    for (const espnId of uniqueEspnIds) {
        const teamData = espnCache.data[espnId];
        const cacheStatus = _isTeamCacheValid(teamData, yearsToScrape);
        if (cacheStatus.isValid) espnCachedCount++;
        else espnNewCount++;
    }
    
    if (verbose && espnNewCount > 0) {
        console.log(`\u001b[32mProcessing ESPN games for ${uniqueEspnIds.length} teams across ${yearsToScrape.length} seasons...\u001b[0m`);
        console.log(`\u001b[36mUsing cached ESPN data: ${espnCachedCount} teams, downloading new data: ${espnNewCount} teams\u001b[0m`);
    }
    
    // --- Collect all ESPN data with unified cache analysis and loading ---
    let allEspnGames = [];
    
    // Create unified progress bar for ESPN processing (cache check + load/download)
    let espnProgress = null;
    if (verbose && uniqueEspnIds.length > 0) {
        const taskName = espnNewCount > 0 ? 'ESPN Data Download' : (espnCacheDate ? `ESPN Data (cached ${espnCacheDate})` : 'ESPN Data (cached)');
        espnProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: uniqueEspnIds.length,
            width: 40,
            complete: '=',
            incomplete: '-',
            renderThrottle: 100,
            clear: false,
            task: taskName
        });
    }
    
    // Process each ESPN ID with unified progress (cache analysis + loading)
    for (let i = 0; i < uniqueEspnIds.length; i++) {
        const espnId = uniqueEspnIds[i];
        const team = initialTeamsData.find(t => t.espn_id === espnId);
        const teamName = team?.short_name || `Team ${espnId}`;
        
        // Check cache status and process accordingly
        const teamData = espnCache.data[espnId];
        const cacheStatus = _isTeamCacheValid(teamData, yearsToScrape);
        
        let teamGames;
        if (cacheStatus.isValid) {
            // Load from cache - cache is keyed by espn_id, add internal team ID
            teamGames = teamData.games
                .filter(game => yearsToScrape.includes(game.season))
                .map(game => ({ ...game, internal_team_id: team.id }));
            if (espnProgress) {
                espnProgress.tick(1, { 
                    task: espnCacheDate ? `ESPN Data (cached ${espnCacheDate})` : 'ESPN Data (cached)',
                    status: `${teamName}`
                });
            }
        } else {
            // Download new data and add internal team ID
            teamGames = await _processEspnTeamGames(
                espnId, 
                team, 
                yearsToScrape, 
                espnCache.data, 
                false, // suppress verbose for individual teams
                espnProgress,
                null // no cache date for downloads
            );
            // Add internal team ID to downloaded games
            teamGames = teamGames.map(game => ({ ...game, internal_team_id: team.id }));
        }
        allEspnGames.push(...teamGames);
    }
    
    if (espnProgress) espnProgress.terminate();
    // Save updated cache once at the end
    cacheManager.set("football_college_espn_schedules", espnCache.data);
    
    // Get teams that have both ESPN and NCAA IDs - these can be matched
    const matchableTeams = initialTeamsData.filter(team => team.espn_id && team.reference_id);
    const espnOnlyTeams = initialTeamsData.filter(team => team.espn_id && !team.reference_id);
    
    // --- Process NCAA schedules for additional game data ---
    // Get unique NCAA IDs for schedule scraping
    const uniqueNcaaIds = [...new Set(
        initialTeamsData
            .filter(team => team.reference_id)
            .map(team => team.reference_id)
    )];
    
    // Count cached vs new NCAA teams with progress indication
    let ncaaCachedCount = 0;
    let ncaaNewCount = 0;
    
    // Create progress bar for cache analysis
    let cacheAnalysisProgress = null;
    if (verbose && uniqueNcaaIds.length > 0) {
        // Move cursor up one line to remove empty space
        process.stdout.write('\u001b[1A');
        cacheAnalysisProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: uniqueNcaaIds.length,
            width: 40,
            complete: '=',
            incomplete: '-',
            renderThrottle: 100,
            clear: false,
            task: 'NCAA Cache Analysis'
        });
    }
    
    // Analyze cache status for each team
    for (const ncaaId of uniqueNcaaIds) {
        const teamNcaaData = getCacheEntryByCanonicalId(ncaaCache.data, ncaaId, 'football');
        const cacheStatus = _isTeamCacheValid(teamNcaaData, yearsToScrape);
        const team = initialTeamsData.find(t => t.reference_id === ncaaId);
        const teamName = team?.short_name || `NCAA ${ncaaId}`;
        
        if (cacheStatus.isValid) {
            ncaaCachedCount++;
            if (cacheAnalysisProgress) {
                cacheAnalysisProgress.tick(1, {
                    task: 'NCAA Cache Analysis',
                    status: `${teamName}`
                });
            }
        } else {
            ncaaNewCount++;
            if (cacheAnalysisProgress) {
                cacheAnalysisProgress.tick(1, {
                    task: 'NCAA Cache Analysis',
                    status: `${teamName} (needs update)`
                });
            }
        }
    }
    
    if (cacheAnalysisProgress) cacheAnalysisProgress.terminate();
    
    let browsers = [], pages = [];
    
    // Initialize browsers for NCAA scraping if needed
    if (ncaaNewCount > 0) {
        const browserCount = Math.min(ncaaNewCount, 3);
        
        try {
            // Progress bar for browser initialization
            let browserProgress = null;
            if (verbose && browserCount > 1) {
                browserProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total browsers \u001b[90m(:elapseds)\u001b[0m', {
                    total: browserCount,
                    width: 40,
                    complete: '=',
                    incomplete: '-',
                    renderThrottle: 100,
                    clear: false,
                    task: 'Browser Setup'
                });
            }
            
            browsers = [];
            for (let i = 0; i < browserCount; i++) {
                try {
                    const browser = await puppeteer.launch({ headless: true });
                    browsers.push(browser);
                    
                    if (browserProgress) {
                        browserProgress.tick(1, {
                            task: 'Browser Setup',
                            status: `Browser ${i + 1} launched`
                        });
                    }
                } catch (error) {
                    if (verbose) console.log(`\u001b[31mFailed to launch browser ${i + 1}: ${error.message}\u001b[0m`);
                }
            }
            
            if (browserProgress) browserProgress.terminate();
            
            // Progress bar for page setup
            let pageProgress = null;
            if (verbose && browsers.length > 1) {
                process.stdout.write('\u001b[1A');
                pageProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
                    total: browsers.length,
                    width: 40,
                    complete: '=',
                    incomplete: '-',
                    renderThrottle: 100,
                    clear: false,
                    task: 'Page Configuration'
                });
            }

            pages = [];
            for (let i = 0; i < browsers.length; i++) {
                try {
                    const browser = browsers[i];
                    const page = await browser.newPage();

                    const browserConfig = getBrowserConfigWithHeaders({
                        'Referer': 'https://stats.ncaa.org/',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'same-origin',
                        'Sec-GPC': '1',
                    });

                    await page.setUserAgent(browserConfig.userAgent);
                    await page.setViewport({ width: 1920, height: 1080 });
                    await page.setExtraHTTPHeaders(browserConfig.headers);
                    await page.goto('https://stats.ncaa.org/', { waitUntil: 'networkidle2' });
                    pages.push(page);
                    
                    if (pageProgress) {
                        pageProgress.tick(1, {
                            task: 'Page Configuration',
                            status: `Page ${i + 1} configured`,
                            success: i + 1,
                            errors: 0
                        });
                    }
                } catch (error) {
                    if (verbose) console.log(`\u001b[31mFailed to configure page ${i + 1}: ${error.message}\u001b[0m`);
                }
            }
            
            if (pageProgress) {
                pageProgress.terminate();
            }
        } catch (error) {
            verbose && console.log(`\u001b[31m--- BROWSER INITIALIZATION FAILED ---\u001b[0m`);
            verbose && console.error(error);
            verbose && console.log(`\u001b[31m-------------------------------------\u001b[0m`);
            if (browsers.length) await Promise.allSettled(browsers.map(b => b.close()));
            browsers = []; pages = [];
        }
    }
    
    // Process teams that need downloading (this may take time but is now accounted for above)
    if (ncaaNewCount > 0) await _processNcaaSchedules(uniqueNcaaIds, initialTeamsData, ncaaCache.data, yearsToScrape, pages, verbose); 
    let allGames = [...allEspnGames]; // Add all ESPN games to the collection
    
    // Deduplicate games BEFORE matching to avoid inflated team game counts
    allGames = _deduplicateGames(allGames);
    
    // Create progress bar for matching process - starts immediately after processing
    let matchProgress = null;
    if (verbose && matchableTeams.length > 0) {
        // Move cursor up one line to remove empty space
        process.stdout.write('\u001b[1A');
        matchProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: matchableTeams.length,
            width: 40,
            complete: '=',
            incomplete: '-',
            renderThrottle: 100,
            clear: false,
            task: 'Team-by-Team Matching'
        });
    }
    
    if (matchableTeams.length > 0) {
        let totalMatched = 0;
        let processedCount = 0;
        for (const team of matchableTeams) {
            // Get ESPN games for this team (already tagged with internal_team_id)
            const teamEspnGames = allGames.filter(game => game.internal_team_id === team.id);
            // Get NCAA games from cache - cache is keyed by reference_id, add internal team ID
            const teamCacheData = getCacheEntryByCanonicalId(ncaaCache.data, team.reference_id, 'football');
            const ncaaGames = (teamCacheData?.games?.filter(game => yearsToScrape.includes(game.season)) || [])
                .map(game => ({ ...game, internal_team_id: team.id }));
            
            // Match games within this team's context
            const beforeMatchCount = teamEspnGames.filter(g => g.reference_id).length;
            _matchTeamGames(teamEspnGames, ncaaGames, team, false, matchProgress); // Pass progress bar
            const afterMatchCount = teamEspnGames.filter(g => g.reference_id).length;
            const newMatches = afterMatchCount - beforeMatchCount;
            totalMatched += newMatches;
            processedCount++;
        }
        
        if (matchProgress) {
            matchProgress.terminate();
        }
    }

    // --- Final processing: legacy matching for remaining unmatched games ---
    const unmatchedGames = allGames.filter(g => !g.reference_id);
    if (verbose && unmatchedGames.length > 0) {
        // Move cursor up one line to remove empty space
        process.stdout.write('\u001b[1A');
        let finalProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: 2,
            width: 40,
            complete: '=',
            incomplete: '-',
            renderThrottle: 100,
            clear: false,
            task: 'Final Processing'
        });
        
        finalProgress.tick(1, {
            task: 'Final Processing',
            status: `Deduplicating ${allGames.length} games`
        });
        
        // Run legacy matching
        allGames = await _matchEspnWithNcaaGames(allGames, ncaaCache.data, initialTeamsData, yearsToScrape, false); // suppress verbose
        
        const finalMatchedCount = allGames.filter(g => g.reference_id).length;
        const finalUnmatchedCount = allGames.length - finalMatchedCount;
        
        finalProgress.tick(1, {
            task: 'Final Processing', 
            status: `${finalMatchedCount} matched, ${finalUnmatchedCount} unmatched games`
        });
        
        finalProgress.terminate();
    } else if (verbose) {
        // Just show deduplication if no legacy matching needed
        // Move cursor up one line to remove empty space  
        process.stdout.write('\u001b[1A');
        let finalProgress = new ProgressBar('\u001b[36m:task\u001b[0m [\u001b[32m:bar\u001b[0m] \u001b[33m:percent\u001b[0m :current/:total \u001b[90m:status\u001b[0m', {
            total: 1,
            width: 40,
            complete: '=',
            incomplete: '-',
            renderThrottle: 100,
            clear: false,
            task: 'Final Processing'
        });
        
        const finalMatchedCount = allGames.filter(g => g.reference_id).length;
        const finalUnmatchedCount = allGames.length - finalMatchedCount;
        
        finalProgress.tick(1, {
            task: 'Final Processing',
            status: `${finalMatchedCount} matched, ${finalUnmatchedCount} unmatched games`
        });
        
        finalProgress.terminate();
    }

    // --- Optional binding discovery and research ---
    const potentialBindings = deducePotentialBindings(allGames, ncaaCache.data, initialTeamsData, yearsToScrape);
    const researchableBindings = potentialBindings.filter(b => !b.already_bound && (b.confidence === 'high' || b.confidence === 'medium'));
    if (researchableBindings.length > 0) {
        process.stdout.write('\u001b[1A');
        verbose && console.log(`\u001b[36mFound ${researchableBindings.length} potential bindings to research...\u001b[0m`);
        
        // If we have browsers available from NCAA downloading, use them
        if (browsers.length > 0 && pages.length > 0) {
            try {
                console.log(`\u001b[35mCalling research function with ${researchableBindings.length} researchable bindings...\u001b[0m`);
                const { researchPotentialNewBindings } = await import('../teams/football-teams-college.js');
                const researchResults = await researchPotentialNewBindings(researchableBindings, verbose, browsers);
            } catch (error) { 
                verbose && console.log(`\u001b[33mWarning: Failed to research bindings: ${error.message}\u001b[0m`); 
            }
        } else {
            // No browsers available - initialize them for binding research
            verbose && console.log(`\u001b[36mInitializing browsers for binding research...\u001b[0m`);
            
            try {
                // Initialize browsers for binding research
                const browserCount = Math.min(3, researchableBindings.length);
                const researchBrowsers = [];
                
                for (let i = 0; i < browserCount; i++) {
                    try {
                        const browser = await puppeteer.launch({ headless: true });
                        researchBrowsers.push(browser);
                    } catch (error) {
                        verbose && console.log(`\u001b[31mFailed to launch research browser ${i + 1}: ${error.message}\u001b[0m`);
                    }
                }
                if (researchBrowsers.length > 0) {
                    verbose && console.log(`\u001b[35mCalling research function with ${researchableBindings.length} researchable bindings...\u001b[0m`);
                    const { researchPotentialNewBindings } = await import('../teams/football-teams-college.js');
                    // Close research browsers
                    await Promise.all(researchBrowsers.map(b => b.close()));
                } else {
                    verbose && console.log(`\u001b[31mCould not initialize browsers for binding research.\u001b[0m`);
                }
            } catch (error) {
                verbose && console.log(`\u001b[33mWarning: Failed to research bindings: ${error.message}\u001b[0m`);
            }
        }
    }
    if (browsers.length > 0) await Promise.all(browsers.map(b => b.close()));
    process.stdout.write('\u001b[1A');
    // --- Finalize and Save ---
    const finalGames = _deduplicateGames(allGames);
    finalGames.forEach(game => { game.id = generateGameId(game.espn_id, game.short_title); });

    if (save) {
        const potentialBindings = deducePotentialBindings(finalGames, ncaaCache.data, initialTeamsData, yearsToScrape);
        _saveUnmatchedReports(finalGames, ncaaCache.data, initialTeamsData, potentialBindings, yearsToScrape, verbose);
        fs.writeFileSync(OUTPUT_FILE, JSON.stringify(finalGames, null, 2), "utf8");
        verbose && console.log(`\u001b[90mCollege Football Games Data Saved To: ${OUTPUT_FILE}\u001b[0m`);
    }
    return finalGames;
}

const _deduplicateGames = (allGames) => {
    const uniqueGames = {};
    allGames.forEach(game => {
        if (game.espn_id) uniqueGames[game.espn_id] = game;
    });
    return Object.values(uniqueGames).sort((a, b) => new Date(a.date_time) - new Date(b.date_time));
};

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
    get_formatted_games();
}