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
import crypto from 'crypto';
import puppeteer from 'puppeteer';

import { getBrowserConfigWithHeaders } from '../util/browser-headers.js';
import cacheManager from '../util/cache-manager.js';
import get_formated_teams from '../teams/football-teams-college.js';

// File to hold formatted data
const OUTPUT_FILE = "data/processed/football-games-college.json";
// Base cache TTL (30 days for global cache)
const BASE_CACHE_TTL = 30 * 24 * 60 * 60 * 1000;
// Browser colors for console output
const BROWSER_COLORS = ['\u001b[33m', '\u001b[34m', '\u001b[32m']; // yellow, blue, green

/**
 * Fetch ESPN games data for a single team and year
 * @param {string} espnId - ESPN team ID
 * @param {number} year - Season year to fetch
 * @param {string} teamName - Team name for logging
 * @param {boolean} verbose - Whether to log progress
 * @returns {Array} Array of game objects for the team/year
 */
const _fetchEspnTeamGames = async (espnId, year, teamName, verbose) => {
    const games = [];
    const scheduleUrl = `https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/${espnId}/schedule?season=${year}`;
    
    try {
        if (verbose) console.log(`\u001b[32mDownloading Schedule: ${scheduleUrl}\u001b[0m`);
        const response = await fetch(scheduleUrl);
        if (!response.ok) return games;
        
        const scheduleJson = await response.json();
        if (!scheduleJson?.events) return games;
        
        // Process each game in the schedule
        for (const game of scheduleJson.events) {
            if (!game.id || !game.competitions || game.competitions.length === 0) continue;
            
            const comp = game.competitions[0];
            const homeComp = comp.competitors?.find(c => c.homeAway === 'home');
            const awayComp = comp.competitors?.find(c => c.homeAway === 'away');
            if (!homeComp || !awayComp) continue;
            
            // Extract scores
            const homeScore = homeComp.score?.value || 0;
            const awayScore = awayComp.score?.value || 0;
            
            // Determine winner
            let winner = null;
            if (homeScore !== null && awayScore !== null) {
                if (homeScore > awayScore) {
                    winner = homeComp.team.id;
                } else if (awayScore > homeScore) {
                    winner = awayComp.team.id;
                }
            }
            
            const gameInfo = {
                espn_id: game.id,
                type: "CFB",
                date_time: game.date,
                season: year,
                title: game.name || null,
                short_title: game.shortName || null,
                venue: comp.venue?.fullName || null,
                home_espn_id: homeComp.team.id,
                away_espn_id: awayComp.team.id,
                home_score: homeScore,
                away_score: awayScore,
                winner: winner,
                play_by_play: `http://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/${game.id}/competitions/${game.id}/plays?lang=en&region=us`
            };
            
            games.push(gameInfo);
        }
        
        // Small delay between requests
        await new Promise(resolve => setTimeout(resolve, 100));
    } catch (error) {
        if (verbose) console.log(`\u001b[33mWarning: Failed to fetch schedule for ${teamName}: ${error.message}\u001b[0m`);
    }
    
    return games;
};

/**
 * Process ESPN games data for a single team with caching
 * @param {string} espnId - ESPN team ID
 * @param {Object} team - Team data object
 * @param {Array<number>} yearsToScrape - Years to scrape data for
 * @param {Object} allSchedulesCache - Cache object for all team schedules
 * @param {boolean} verbose - Whether to log progress
 * @returns {Array} Array of games for the team
 */
const _processEspnTeamGames = async (espnId, team, yearsToScrape, allSchedulesCache, verbose) => {
    const teamName = team ? team.short_name : `Team ${espnId}`;
    let teamGames = [];
    
    // Check if this team's data is cached and valid
    const teamData = allSchedulesCache[espnId];
    const cacheStatus = _isTeamCacheValid(teamData, yearsToScrape);
    
    if (cacheStatus.isValid) {
        // Filter cached games to only include years we're processing
        teamGames = teamData.games.filter(game => yearsToScrape.includes(game.season));
    } else {
        // Start with existing games if available, filtered to configured years
        if (teamData && teamData.games) {
            teamGames = teamData.games.filter(game => yearsToScrape.includes(game.season));
            
            // Remove games from years we're about to re-download to avoid duplicates
            if (cacheStatus.needsCurrentYearOnly) {
                const currentYear = Math.max(...yearsToScrape);
                teamGames = teamGames.filter(game => game.season !== currentYear);
                if (verbose) console.log(`\u001b[33mUpdating current year (${currentYear}) for ${teamName}\u001b[0m`);
            } else {
                // Remove games from all missing years
                teamGames = teamGames.filter(game => !cacheStatus.missingYears.includes(game.season));
                if (verbose) console.log(`\u001b[33mDownloading missing years [${cacheStatus.missingYears.join(', ')}] for ${teamName}\u001b[0m`);
            }
        } else {
            teamGames = [];
        }
        
        // Fetch games for missing years only
        const yearsToFetch = cacheStatus.missingYears.length > 0 ? cacheStatus.missingYears : yearsToScrape;
        for (const year of yearsToFetch) {
            const newGames = await _fetchEspnTeamGames(espnId, year, teamName, verbose);
            teamGames.push(...newGames);
        }
        
        // Calculate and store absolute expiration time for this team
        const expiresAt = _calculateExpirationTime(teamGames);
        allSchedulesCache[espnId] = {
            games: teamGames,
            savedAt: Date.now(),
            expiresAt: expiresAt
        };
        
        // Save cache immediately after each team to prevent data loss
        cacheManager.set("football_college_espn_schedules", allSchedulesCache);
    }
    
    return teamGames;
};

/**
 * Scrape NCAA schedule for a team with session establishment
 * @param {string} ncaaId - NCAA team ID
 * @param {Array<number>} years - Years to scrape
 * @param {Object} page - Puppeteer page instance
 * @param {boolean} verbose - Whether to log progress
 * @param {number} pageIndex - Browser page index for colored logging
 * @returns {Array} Array of games with NCAA opponent IDs
 */
const _scrapeNcaaTeamSchedule = async (ncaaId, years, page, verbose, pageIndex = 0) => {
    const games = [];
    const teamUrl = `https://stats.ncaa.org/teams/${ncaaId}`;
    
    try {
        if (verbose) console.log(`${BROWSER_COLORS[pageIndex]}Visiting NCAA team page: ${teamUrl}\u001b[0m`);
        await page.goto(teamUrl, { waitUntil: 'networkidle2', timeout: 30000 });
        
        // Check for access denied and establish session if needed
        const bodyText = await page.evaluate(() => document.body.textContent);
        if (bodyText.includes('Access Denied') || bodyText.includes('403')) {
            if (verbose) console.log(`\u001b[31mAccess denied for ${teamUrl}, establishing session...\u001b[0m`);
            await new Promise(resolve => setTimeout(resolve, 5000));
            await page.goto('https://stats.ncaa.org/', { waitUntil: 'networkidle2' });
            await page.goto(teamUrl, { waitUntil: 'networkidle2', timeout: 30000 });
        }
        
        // Get the year dropdown options that match the years we want to scrape
        const yearSelect = await page.$('#year_list');
        if (yearSelect) {
            // Get year options that match our target years
            const yearOptions = await page.evaluate((selectElement, targetYears) => {
                const options = Array.from(selectElement.options);
                return options
                    .map(option => ({
                        value: option.value,
                        text: option.text
                    }))
                    .filter(option => {
                        // Extract year from option text (e.g., "2024-25" -> 2024)
                        const seasonYear = parseInt(option.text.split('-')[0]);
                        return targetYears.includes(seasonYear);
                    });
            }, yearSelect, years);
            
            // Process each year option
            for (const yearOption of yearOptions) {
                try {
                    if (verbose) console.log(`${BROWSER_COLORS[pageIndex]}Processing year ${yearOption.text} (${yearOption.value}) for NCAA ID ${ncaaId}\u001b[0m`);
                    
                    // Select the year option and wait for navigation
                    await page.select('#year_list', yearOption.value);
                    await page.waitForNavigation({ waitUntil: 'networkidle2' });
                    
                    // Extract the year from the option text (e.g., "2024-25" -> 2024)
                    const seasonYear = parseInt(yearOption.text.split('-')[0]);
                    
                    // Look for schedule table specifically (not statistics tables)
                    const tables = await page.$$('table');
                    let scheduleRows = [];
                    
                    for (let tableIndex = 0; tableIndex < tables.length; tableIndex++) {
                        const table = tables[tableIndex];
                        const rows = await table.$$('tr');
                        if (rows.length < 2) continue;
                        
                        // Check header row (row 0) for "Date" header
                        const headerRow = rows[0];
                        const headerCells = await headerRow.$$('td, th');
                        if (headerCells.length > 0) {
                            const firstHeaderText = await headerCells[0].evaluate(el => el.textContent.trim());
                            // Look for "Date" header - this identifies the schedule table
                            if (firstHeaderText.toLowerCase() === 'date') {
                                scheduleRows = rows;
                                break;
                            }
                        }
                    }
                    
                    for (let i = 1; i < scheduleRows.length; i++) { // Skip header row
                        const row = scheduleRows[i];
                        const cells = await row.$$('td');
                        if (cells.length >= 3) {
                            // Extract game information
                            const dateCell = cells[0] ? await cells[0].evaluate(el => el.textContent.trim()) : '';
                            const opponentCell = cells[1] ? await cells[1] : null;
                            const scoreCell = cells[2] ? await cells[2] : null;
                            
                            // Extract opponent NCAA ID from link
                            let opponentNcaaId = null;
                            if (opponentCell) {
                                const opponentLink = await opponentCell.$('a[href*="/teams/"]');
                                if (opponentLink) {
                                    const href = await opponentLink.evaluate(el => el.href);
                                    const match = href.match(/\/teams\/(\d+)/);
                                    if (match) opponentNcaaId = match[1];
                                }
                            }
                            const opponentName = opponentCell ? await opponentCell.evaluate(el => el.textContent.trim()) : '';
                            
                            // Extract NCAA game ID from box score link
                            let ncaaGameId = null;
                            const scoreText = scoreCell ? await scoreCell.evaluate(el => el.textContent.trim()) : '';
                            if (scoreCell) {
                                const boxScoreLink = await scoreCell.$('a[href*="/contests/"]');
                                if (boxScoreLink) {
                                    const href = await boxScoreLink.evaluate(el => el.href);
                                    const match = href.match(/\/contests\/(\d+)/);
                                    if (match) ncaaGameId = match[1];
                                }
                            }
                            
                            if (dateCell && opponentName) {
                                games.push({
                                    date: dateCell,
                                    opponent_name: opponentName,
                                    opponent_ncaa_id: opponentNcaaId,
                                    ncaa_game_id: ncaaGameId,
                                    score: scoreText,
                                    season: seasonYear,
                                    home_team_ncaa_id: ncaaId
                                });
                            }
                        }
                    }
                } catch (yearError) {
                    if (verbose) console.log(`\u001b[33mWarning: Failed to process year ${yearOption.text} for NCAA ID ${ncaaId}: ${yearError.message}\u001b[0m`);
                }
            }
        }
    } catch (error) {
        if (verbose) console.log(`\u001b[33mWarning: Failed to scrape NCAA schedule for ${ncaaId}: ${error.message}\u001b[0m`);
    }
    
    return games;
};

/**
 * Calculate absolute expiration time for a team based on their next upcoming game
 * @param {Array<Object>} games - Array of games for the team
 * @returns {number} Absolute expiration time in milliseconds (1 hour after next game, minimum 30 days from now)
 */
const _calculateExpirationTime = (games) => {
    if (!games || games.length === 0) return Date.now() + BASE_CACHE_TTL; 
    const now = Date.now();
    
    // Find the next upcoming game (earliest game that is in the future)
    const upcomingGames = games
        .filter(game => game.date_time)
        .map(game => new Date(game.date_time))
        .filter(gameDate => gameDate.getTime() > now)
        .sort((a, b) => a.getTime() - b.getTime());
    
    if (upcomingGames.length > 0) {
        // Set expiry to 1 hour after the next upcoming game
        const nextGame = upcomingGames[0];
        const oneHourAfterNextGame = nextGame.getTime() + (60 * 60 * 1000);
        return oneHourAfterNextGame;
    }
    
    // If no upcoming games, use default TTL of 1 month from now
    // TTL should never be less than current time to avoid unnecessary updates
    return now + BASE_CACHE_TTL;
};

/**
 * Check if cached team data has expired based on stored expiration time and covers all required years
 * @param {Object} teamData - Team data with games, savedAt timestamp, and expiresAt
 * @param {Array<number>} requiredYears - Years that must be present in the cache
 * @returns {Object} { isValid: boolean, missingYears: Array<number>, needsCurrentYearOnly: boolean }
 */
const _isTeamCacheValid = (teamData, requiredYears) => {
    if (!teamData || !teamData.games || !teamData.savedAt || !teamData.expiresAt) {
        return { isValid: false, missingYears: requiredYears, needsCurrentYearOnly: false };
    }
    if (Array.isArray(teamData.games) && teamData.games.length === 0) {
        return { isValid: false, missingYears: requiredYears, needsCurrentYearOnly: false };
    }
    
    // Check if cache has expired
    const isExpired = Date.now() >= teamData.expiresAt;
    
    // Check which years are present in the cached games
    const cachedYears = new Set();
    teamData.games.forEach(game => {
        if (game.season) cachedYears.add(game.season);
    });
    
    const missingYears = requiredYears.filter(year => !cachedYears.has(year));
    
    if (isExpired) {
        // If expired but has some years, only download current year
        const currentYear = Math.max(...requiredYears);
        if (missingYears.length < requiredYears.length) {
            return { isValid: false, missingYears: [currentYear], needsCurrentYearOnly: true };
        } else {
            return { isValid: false, missingYears: requiredYears, needsCurrentYearOnly: false };
        }
    }
    
    // If not expired, check for missing years
    if (missingYears.length > 0) {
        return { isValid: false, missingYears: missingYears, needsCurrentYearOnly: false };
    }
    
    return { isValid: true, missingYears: [], needsCurrentYearOnly: false };
};

/**
 * Remove duplicate games by espn_id since we get the same game from both teams' schedules
 * @param {Array} allGames - Array of all games from different sources
 * @returns {Array} Array of unique games sorted by date
 */
const _deduplicateGames = (allGames) => {
    const uniqueGames = {};
    allGames.forEach(game => {
        if (game.espn_id && !uniqueGames[game.espn_id]) uniqueGames[game.espn_id] = game;
    });
    return Object.values(uniqueGames).sort((a, b) => new Date(a.date_time) - new Date(b.date_time));
};

/**
 * Match ESPN games with NCAA games to add reference IDs and internal team IDs
 * @param {Array} espnGames - Array of ESPN games
 * @param {Object} ncaaSchedulesCache - NCAA schedules cache
 * @param {Array} teamsData - Teams data with ESPN and NCAA IDs
 * @param {Array<number>} yearsToScrape - Years being processed
 * @param {boolean} verbose - Whether to log progress
 * @returns {Array} Array of ESPN games with NCAA references added
 */
const _matchEspnWithNcaaGames = async (espnGames, ncaaSchedulesCache, teamsData, yearsToScrape, verbose) => {
    // Import consolidation utilities
    const { getCanonicalNcaaId, consolidateGamesNcaaIds, consolidateTeamsNcaaIds } = await import('../util/ncaa-id-consolidation.js');
    
    // Consolidate NCAA IDs in teams data to handle duplicates
    const consolidatedTeamsData = consolidateTeamsNcaaIds(teamsData);
    // Flatten all NCAA games from cache, filtered to configured years
    const allNcaaGames = [];
    Object.values(ncaaSchedulesCache).forEach(teamCache => {
        if (teamCache.games && Array.isArray(teamCache.games)) {
            const filteredGames = teamCache.games.filter(game => yearsToScrape.includes(game.season));
            allNcaaGames.push(...filteredGames);
        }
    });

    // Consolidate NCAA IDs in all NCAA games to handle duplicates
    const consolidatedNcaaGames = consolidateGamesNcaaIds(allNcaaGames);

    // Deduplicate consolidated NCAA games by ncaa_game_id
    const uniqueNcaaGames = {};
    consolidatedNcaaGames.forEach(game => {
        if (game.ncaa_game_id && !uniqueNcaaGames[game.ncaa_game_id]) {
            uniqueNcaaGames[game.ncaa_game_id] = game;
        }
    });
    const ncaaGames = Object.values(uniqueNcaaGames);

    // Filter ESPN games to only include those with dates in the past (completed games)
    const now = new Date();
    const completedEspnGames = espnGames.filter(game => {
        if (!game.date_time) return false;
        return new Date(game.date_time) < now;
    });

    const futureGamesCount = espnGames.length - completedEspnGames.length;
    const duplicatesRemoved = consolidatedNcaaGames.length - ncaaGames.length;

    if (verbose) console.log(`\u001b[32mMatching ${completedEspnGames.length} completed ESPN games with ${ncaaGames.length} NCAA games (${futureGamesCount} future games and ${duplicatesRemoved} duplicates removed)...\u001b[0m`);

    let matchCount = 0;
    const matchedNcaaGameIds = new Set();
    let skippedDuplicateMatches = 0;
    
    completedEspnGames.forEach(espnGame => {
        const espnDate = new Date(espnGame.date_time);
        
        // Find ESPN teams in our consolidated teams data
        const homeTeam = consolidatedTeamsData.find(t => t.espn_id === espnGame.home_espn_id);
        const awayTeam = consolidatedTeamsData.find(t => t.espn_id === espnGame.away_espn_id);
        
        if (!homeTeam || !awayTeam) return;

        // Look for matching NCAA game within date window that hasn't been used yet
        const matchingNcaaGame = ncaaGames.find(ncaaGame => {
            if (!ncaaGame.date || !ncaaGame.ncaa_game_id) return false;
            
            // Skip if this NCAA game ID has already been matched
            if (matchedNcaaGameIds.has(ncaaGame.ncaa_game_id)) {
                // Check if this would have been a valid match (for logging purposes)
                let ncaaDate;
                try {
                    const dateStr = ncaaGame.date.toString();
                    if (dateStr.includes('/')) {
                        const [datePart] = dateStr.split(' ');
                        const [month, day, year] = datePart.split('/');
                        ncaaDate = new Date(year, month - 1, day);
                    } else {
                        ncaaDate = new Date(ncaaGame.date);
                    }
                    
                    if (!isNaN(ncaaDate.getTime())) {
                        const daysDiff = Math.abs((espnDate - ncaaDate) / (1000 * 60 * 60 * 24));
                        const espnTeamIds = new Set([homeTeam.ncaa_id, awayTeam.ncaa_id]);
                        
                        let wouldMatch = false;
                        if (daysDiff < 2.5) {
                            wouldMatch = homeTeam.ncaa_id && awayTeam.ncaa_id && 
                                       (espnTeamIds.has(ncaaGame.home_team_ncaa_id) || espnTeamIds.has(ncaaGame.opponent_ncaa_id));
                        } else if (daysDiff <= 7) {
                            wouldMatch = homeTeam.ncaa_id && awayTeam.ncaa_id && 
                                       espnTeamIds.has(ncaaGame.home_team_ncaa_id) && 
                                       espnTeamIds.has(ncaaGame.opponent_ncaa_id);
                        }
                        
                        if (wouldMatch) {
                            skippedDuplicateMatches++;
                        }
                    }
                } catch (error) {
                    // Ignore parsing errors for logging
                }
                return false;
            }
            
            // Parse NCAA date
            let ncaaDate;
            try {
                const dateStr = ncaaGame.date.toString();
                if (dateStr.includes('/')) {
                    const [datePart] = dateStr.split(' ');
                    const [month, day, year] = datePart.split('/');
                    ncaaDate = new Date(year, month - 1, day);
                } else {
                    ncaaDate = new Date(ncaaGame.date);
                }
            } catch (error) {
                return false;
            }
            
            if (isNaN(ncaaDate.getTime())) return false;
            
            const daysDiff = Math.abs((espnDate - ncaaDate) / (1000 * 60 * 60 * 24));
            const espnTeamIds = new Set([homeTeam.ncaa_id, awayTeam.ncaa_id]);
            const ncaaTeamIds = new Set([ncaaGame.home_team_ncaa_id, ncaaGame.opponent_ncaa_id]);
            
            if (daysDiff < 2.5) {
                // Same day: only need 1 team to match
                return homeTeam.ncaa_id && awayTeam.ncaa_id && 
                       (espnTeamIds.has(ncaaGame.home_team_ncaa_id) || espnTeamIds.has(ncaaGame.opponent_ncaa_id));
            } else if (daysDiff <= 7) {
                // Different days (within 7 days): both teams must match
                return homeTeam.ncaa_id && awayTeam.ncaa_id && 
                       espnTeamIds.has(ncaaGame.home_team_ncaa_id) && 
                       espnTeamIds.has(ncaaGame.opponent_ncaa_id);
            } else {
                return false;
            }
        });

        if (matchingNcaaGame) {
            espnGame.reference_id = matchingNcaaGame.ncaa_game_id;
            matchedNcaaGameIds.add(matchingNcaaGame.ncaa_game_id);
            matchCount++;
        }
        
        // Add internal team IDs regardless of NCAA match
        espnGame.home_id = homeTeam.id;
        espnGame.away_id = awayTeam.id;
        espnGame.home = homeTeam.name;
        espnGame.away = awayTeam.name;
        
        // Update winner to use internal ID if available
        if (espnGame.winner) {
            if (espnGame.winner === espnGame.home_espn_id && homeTeam.id) {
                espnGame.winner = homeTeam.id;
            } else if (espnGame.winner === espnGame.away_espn_id && awayTeam.id) {
                espnGame.winner = awayTeam.id;
            }
        }
    });
    
    if (verbose) {
        console.log(`\u001b[36mMatched ${matchCount} games with NCAA data out of ${completedEspnGames.length} completed games\u001b[0m`);
        if (skippedDuplicateMatches > 0) {
            console.log(`\u001b[33mPrevented ${skippedDuplicateMatches} duplicate NCAA game ID matches (enforcing 1:1 mapping)\u001b[0m`);
        }
    }
    return espnGames;
};

/**
 * College Football Games
 *
 * Retrieves college football game data from ESPN's API for each team. The combined data
 * is processed into a structured array and saved to a JSON file. Each team's data has
 * individual TTL based on their most recent game time.
 *
 * @param {boolean} verbose - Whether to print progress messages (default: true)
 * @param {boolean} save - Whether to save data to data/processed folder
 * 
 * @returns {Array} Array containing game information:
 *  * id [string] - A generated unique identifier for each game
 *  * espn_id [string] - ESPN-assigned game ID
 *  * type [string] - Sport type code ("CFB" for college football)
 *  * date_time [string] - ISO date and time of the game
 *  * season [number] - Season year
 *  * title [string] - Full title of the game
 *  * short_title [string] - Shortened title of the game
 *  * venue [string] - Venue where the game is played
 *  * home_espn_id [string] - ESPN ID for the home team
 *  * away_espn_id [string] - ESPN ID for the away team
 *  * home_score [number] - Home team score (null if not completed)
 *  * away_score [number] - Away team score (null if not completed)
 *  * winner [string] - ESPN ID of the winning team (null if tie/not completed)
 *  * play_by_play [string] - URL for play-by-play data
 */
async function get_formatted_games(verbose = true, save = true) {
    // Get current year and create year range: current year - 1 through current year - 2
    const currentYear = new Date().getFullYear();
    const yearsToScrape = [currentYear, currentYear - 1, currentYear - 2];
    // Load teams data using the existing function with caching
    const teamsData = await get_formated_teams(verbose, save);
    // Get unique ESPN IDs from teams data
    const uniqueEspnIds = [...new Set(
        teamsData
            .filter(team => team.espn_id)
            .map(team => team.espn_id)
    )];
    // Load all cached schedules
    const cacheWrapper = cacheManager.get("football_college_espn_schedules", BASE_CACHE_TTL);
    let allSchedulesCache = (cacheWrapper && cacheWrapper.data) ? cacheWrapper.data : {};
    const allGames = [];
    
    // Log ESPN cache status
    if (cacheWrapper && cacheWrapper.savedAt && verbose) {
        const savedDate = new Date(cacheWrapper.savedAt);
        console.log(`\u001b[36mUsing cached ESPN schedules data (from ${savedDate.toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit', hour12: true })})\u001b[0m`);
    }
    
    // Count cached vs new ESPN teams
    let espnCachedCount = 0;
    let espnNewCount = 0;

    
    for (const espnId of uniqueEspnIds) {
        const teamData = allSchedulesCache[espnId];
        const cacheStatus = _isTeamCacheValid(teamData, yearsToScrape);
        if (cacheStatus.isValid) espnCachedCount++;
        else espnNewCount++;
    }
    
    if (verbose && espnNewCount > 0) {
        console.log(`\u001b[32mProcessing ESPN games for ${uniqueEspnIds.length} teams across ${yearsToScrape.length} seasons...\u001b[0m`);
        console.log(`\u001b[36mUsing cached ESPN data: ${espnCachedCount} teams, downloading new data: ${espnNewCount} teams\u001b[0m`);
    }
    
    
    // Process each ESPN ID with individual caching
    for (let i = 0; i < uniqueEspnIds.length; i++) {
        const espnId = uniqueEspnIds[i];
        const team = teamsData.find(t => t.espn_id === espnId);
        
        const teamGames = await _processEspnTeamGames(
            espnId, 
            team, 
            yearsToScrape, 
            allSchedulesCache, 
            verbose
        );
        
        allGames.push(...teamGames);
    }
    // Save updated cache once at the end
    cacheManager.set("football_college_espn_schedules", allSchedulesCache);

    
    // Now process NCAA schedules for additional game data
    // Get unique NCAA IDs for schedule scraping
    const uniqueNcaaIds = [...new Set(
        teamsData
            .filter(team => team.ncaa_id)
            .map(team => team.ncaa_id)
    )];
    
    // Load NCAA schedules cache
    const ncaaWrapper = cacheManager.get("football_college_ncaa_schedules", BASE_CACHE_TTL);
    let ncaaSchedulesCache = (ncaaWrapper && ncaaWrapper.data) ? ncaaWrapper.data : {};
    
    // Log NCAA cache status
    if (ncaaWrapper && ncaaWrapper.savedAt && verbose) {
        const savedDate = new Date(ncaaWrapper.savedAt);
        console.log(`\u001b[36mUsing cached NCAA schedules data (from ${savedDate.toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit', hour12: true })})\u001b[0m`);
    }
    
    // Count cached vs new NCAA teams
    let ncaaCachedCount = 0;
    let ncaaNewCount = 0;
    
    for (const ncaaId of uniqueNcaaIds) {
        const teamNcaaData = ncaaSchedulesCache[ncaaId];
        const cacheStatus = _isTeamCacheValid(teamNcaaData, yearsToScrape);
        if (cacheStatus.isValid) ncaaCachedCount++;
        else ncaaNewCount++;
    }
    
    // Initialize browsers for NCAA scraping only if needed
    let browsers = [];
    let pages = [];
    if (ncaaNewCount > 0) {
        try {
            // Create only as many browsers as needed (max 3)
            const browserCount = Math.min(ncaaNewCount, 3);
            const browserPromises = [];
            for (let i = 0; i < browserCount; i++) {
                browserPromises.push(puppeteer.launch({ headless: true }));
            }
            browsers = await Promise.all(browserPromises);
            
            // Set up pages with proper headers and configuration
            for (let i = 0; i < browsers.length; i++) {
                const page = await browsers[i].newPage();
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
                pages.push(page);
            }
            
            // Establish sessions for all browsers
            for (let i = 0; i < pages.length; i++) {
                try {
                    if (verbose) console.log(`${BROWSER_COLORS[i]}Browser ${i + 1} establishing NCAA session...\u001b[0m`);
                    await pages[i].goto('https://stats.ncaa.org/', { waitUntil: 'networkidle2' });
                } catch (error) {
                    if (verbose) console.log(`\u001b[31mFailed to establish session for browser ${i + 1}: ${error.message}\u001b[0m`);
                }
            }
        } catch (error) {
            if (verbose) console.log(`\u001b[31mFailed to initialize browsers: ${error.message}\u001b[0m`);
        }
    }
    
    // Process NCAA schedules with concurrent browsers
    if (verbose && ncaaNewCount > 0) {
        console.log(`\u001b[32mProcessing NCAA schedules for ${uniqueNcaaIds.length} teams...\u001b[0m`);
        console.log(`\u001b[36mUsing cached NCAA data: ${ncaaCachedCount} teams, downloading new data: ${ncaaNewCount} teams\u001b[0m`);
    }
    
    for (let i = 0; i < uniqueNcaaIds.length; i++) {
        const ncaaId = uniqueNcaaIds[i];
        const team = teamsData.find(t => t.ncaa_id === ncaaId);
        const teamName = team ? team.short_name : `NCAA ${ncaaId}`;
        
        // Check if this team's NCAA data is cached and valid
        const teamNcaaData = ncaaSchedulesCache[ncaaId];
        const cacheStatus = _isTeamCacheValid(teamNcaaData, yearsToScrape);
        
        if (cacheStatus.isValid) {
            // Filter cached games to only include years we're processing
            teamNcaaData.games = teamNcaaData.games.filter(game => yearsToScrape.includes(game.season));
            // Using cached data silently
        } else if (pages.length > 0) {
            // Use round-robin browser assignment
            const pageIndex = i % pages.length;
            const page = pages[pageIndex];
            const browserColor = BROWSER_COLORS[pageIndex];
            
            // Determine which years to scrape
            const yearsToFetch = cacheStatus.missingYears.length > 0 ? cacheStatus.missingYears : yearsToScrape;
            
            if (cacheStatus.needsCurrentYearOnly) {
                const currentYear = Math.max(...yearsToScrape);
                if (verbose) console.log(`${browserColor}Updating current year (${currentYear}) NCAA schedule for ${teamName} (NCAA ID: ${ncaaId})...\u001b[0m`);
            } else if (cacheStatus.missingYears.length > 0 && cacheStatus.missingYears.length < yearsToScrape.length) {
                if (verbose) console.log(`${browserColor}Downloading missing years [${cacheStatus.missingYears.join(', ')}] NCAA schedule for ${teamName} (NCAA ID: ${ncaaId})...\u001b[0m`);
            } else {
                if (verbose) console.log(`${browserColor}Scraping NCAA schedule for ${teamName} (NCAA ID: ${ncaaId})...\u001b[0m`);
            }
            
            try {
                const ncaaGames = await _scrapeNcaaTeamSchedule(ncaaId, yearsToFetch, page, verbose, pageIndex);
                
                // Start with existing games if available and not doing a full refresh
                let allGames = [];
                if (teamNcaaData && teamNcaaData.games && cacheStatus.missingYears.length > 0) {
                    // Keep existing games from years we're not re-downloading, filtered to configured years
                    allGames = teamNcaaData.games.filter(game => yearsToScrape.includes(game.season) && !yearsToFetch.includes(game.season));
                    // Add newly scraped games
                    allGames.push(...ncaaGames);
                } else {
                    // Full refresh or no existing data
                    allGames = ncaaGames;
                }
                
                const gamesWithDateTime = allGames.map(g => {
                    if (!g.date) return { date_time: null };
                    try {
                        return { date_time: new Date(g.date).toISOString() };
                    } catch (error) {
                        return { date_time: null };
                    }
                });
                const expiresAt = _calculateExpirationTime(gamesWithDateTime);
                ncaaSchedulesCache[ncaaId] = {
                    games: allGames,
                    savedAt: Date.now(),
                    expiresAt: expiresAt
                };
                // Save NCAA cache immediately after each team to prevent data loss
                cacheManager.set("football_college_ncaa_schedules", ncaaSchedulesCache);
                // Delay between requests
                await new Promise(resolve => setTimeout(resolve, 1000));
            } catch (error) {
                // Check if the error is due to detached frame
                if (error.message.includes('detached') || error.message.includes('Navigating frame was detached')) {
                    if (verbose) console.log(`${browserColor}Browser frame detached for ${teamName}, recreating browser instance...\u001b[0m`);
                    
                    try {
                        // Close the current browser and create a new one
                        await browsers[pageIndex].close();
                        browsers[pageIndex] = await puppeteer.launch({ headless: true });
                        pages[pageIndex] = await browsers[pageIndex].newPage();
                        await getBrowserConfigWithHeaders(pages[pageIndex]);
                        
                        // Retry the scraping with the new browser instance
                        const ncaaGames = await _scrapeNcaaTeamSchedule(ncaaId, yearsToScrape, pages[pageIndex], verbose, pageIndex);
                        
                        const gamesWithDateTime = ncaaGames.map(g => {
                            if (!g.date) return { date_time: null };
                            try {
                                return { date_time: new Date(g.date).toISOString() };
                            } catch (error) {
                                return { date_time: null };
                            }
                        });
                        const expiresAt = _calculateExpirationTime(gamesWithDateTime);
                        ncaaSchedulesCache[ncaaId] = {
                            games: ncaaGames,
                            savedAt: Date.now(),
                            expiresAt: expiresAt
                        };
                        // Save NCAA cache immediately after each team to prevent data loss
                        cacheManager.set("football_college_ncaa_schedules", ncaaSchedulesCache);
                        // Delay between requests
                        await new Promise(resolve => setTimeout(resolve, 1000));
                        
                        if (verbose) console.log(`${browserColor}Successfully recovered from browser detachment for ${teamName}\u001b[0m`);
                    } catch (retryError) {
                        if (verbose) console.log(`\u001b[33mWarning: Failed to recover from browser detachment for ${teamName}: ${retryError.message}\u001b[0m`);
                    }
                } else {
                    if (verbose) console.log(`\u001b[33mWarning: Failed to scrape NCAA schedule for ${teamName}: ${error.message}\u001b[0m`);
                }
            }
        }
    }
    
    // Save NCAA schedules cache once at the end
    cacheManager.set("football_college_ncaa_schedules", ncaaSchedulesCache);
    
    // Remove duplicates by espn_id since we get the same game from both teams' schedules
    const games = _deduplicateGames(allGames);


    // Apply matching
    const matchedGames = await _matchEspnWithNcaaGames(games, ncaaSchedulesCache, teamsData, yearsToScrape, verbose);
    
    /**
     * Deduce potential ESPN->NCAA bindings from partially matched games
     * Analyzes games where one team is bound (known) and attempts to find
     * the corresponding NCAA team for the unbound ESPN team
     */
    const deducePotentialBindings = (espnGames, ncaaSchedulesCache, teamsData) => {
        const potentialBindings = [];
        const seenBindings = new Set();
        
        // Flatten all NCAA games for analysis
        const allNcaaGames = [];
        Object.values(ncaaSchedulesCache).forEach(teamCache => {
            if (teamCache.games && Array.isArray(teamCache.games)) {
                const filteredGames = teamCache.games.filter(game => yearsToScrape.includes(game.season));
                allNcaaGames.push(...filteredGames);
            }
        });
        
        // Find ESPN games with exactly one unbound team
        espnGames.forEach(espnGame => {
            if (!espnGame.date_time) return;
            
            const homeTeam = teamsData.find(t => t.espn_id === espnGame.home_espn_id);
            const awayTeam = teamsData.find(t => t.espn_id === espnGame.away_espn_id);
            
            let unboundEspnId = null;
            let boundNcaaId = null;
            let unboundIsHome = false;
            
            // Check if exactly one team is unbound
            if (!homeTeam && awayTeam && awayTeam.ncaa_id) {
                unboundEspnId = espnGame.home_espn_id;
                boundNcaaId = awayTeam.ncaa_id;
                unboundIsHome = true;
            } else if (!awayTeam && homeTeam && homeTeam.ncaa_id) {
                unboundEspnId = espnGame.away_espn_id;
                boundNcaaId = homeTeam.ncaa_id;
                unboundIsHome = false;
            }
            
            if (!unboundEspnId || !boundNcaaId) return;
            
            // Parse ESPN game date
            const espnDate = new Date(espnGame.date_time);
            if (isNaN(espnDate.getTime())) return;
            
            // Find matching NCAA games for the bound team
            const matchingNcaaGames = allNcaaGames.filter(ncaaGame => {
                if (!ncaaGame.date) return false;
                
                // Check if this NCAA game involves the bound team
                const involvesBoundTeam = 
                    ncaaGame.home_team_ncaa_id === boundNcaaId || 
                    ncaaGame.opponent_ncaa_id === boundNcaaId;
                
                if (!involvesBoundTeam) return false;
                
                // Parse NCAA date
                let ncaaDate;
                try {
                    if (ncaaGame.date.includes('/')) {
                        const [month, day, year] = ncaaGame.date.split('/').map(Number);
                        ncaaDate = new Date(year, month - 1, day);
                    } else {
                        ncaaDate = new Date(ncaaGame.date);
                    }
                } catch (error) {
                    return false;
                }
                
                if (isNaN(ncaaDate.getTime())) return false;
                
                // Check date proximity (within 2 days)
                const daysDiff = Math.abs((espnDate - ncaaDate) / (1000 * 60 * 60 * 24));
                return daysDiff < 2.5;
            });
            
            // For each matching NCAA game, identify the opponent NCAA ID
            matchingNcaaGames.forEach(ncaaGame => {
                let opponentNcaaId = null;
                
                if (ncaaGame.home_team_ncaa_id === boundNcaaId) {
                    opponentNcaaId = ncaaGame.opponent_ncaa_id;
                } else if (ncaaGame.opponent_ncaa_id === boundNcaaId) {
                    opponentNcaaId = ncaaGame.home_team_ncaa_id;
                }
                
                if (!opponentNcaaId) return;
                
                // Check if this NCAA ID is already bound to a different ESPN team
                const existingTeamWithNcaaId = teamsData.find(team => team.ncaa_id === opponentNcaaId);
                const isAlreadyBound = existingTeamWithNcaaId ? true : false;
                const existingEspnId = existingTeamWithNcaaId ? existingTeamWithNcaaId.espn_id : null;
                
                // Create unique key to avoid duplicates
                const bindingKey = `${unboundEspnId}-${opponentNcaaId}`;
                if (seenBindings.has(bindingKey)) return;
                seenBindings.add(bindingKey);
                
                // Find NCAA team details
                const ncaaTeam = Object.values(ncaaSchedulesCache).find(teamCache => 
                    teamCache.games && teamCache.games.some(g => 
                        g.home_team_ncaa_id === opponentNcaaId || 
                        g.opponent_ncaa_id === opponentNcaaId
                    )
                );
                
                const ncaaGameWithTeam = ncaaTeam?.games?.find(g => 
                    g.home_team_ncaa_id === opponentNcaaId || g.opponent_ncaa_id === opponentNcaaId
                );
                
                let ncaaTeamName = 'Unknown NCAA Team';
                if (ncaaGameWithTeam) {
                    if (ncaaGameWithTeam.home_team_ncaa_id === opponentNcaaId) {
                        ncaaTeamName = ncaaGameWithTeam.home_team || 'Unknown NCAA Team';
                    } else if (ncaaGameWithTeam.opponent_team) {
                        // Extract team name from opponent field, removing @ prefix
                        ncaaTeamName = ncaaGameWithTeam.opponent_team.replace(/^@\s*/, '');
                    }
                }
                
                potentialBindings.push({
                    espn_id: unboundEspnId,
                    ncaa_id: opponentNcaaId,
                    espn_game_id: espnGame.espn_id,
                    espn_game_date: espnGame.date_time,
                    espn_game_title: espnGame.title,
                    ncaa_game_id: ncaaGame.ncaa_game_id,
                    ncaa_game_date: ncaaGame.date,
                    ncaa_team_name: ncaaTeamName,
                    bound_team_name: unboundIsHome ? 
                        (awayTeam?.displayName || awayTeam?.name || 'Unknown') : 
                        (homeTeam?.displayName || homeTeam?.name || 'Unknown'),
                    confidence: 'medium',
                    already_bound: isAlreadyBound,
                    existing_espn_id: existingEspnId
                });
            });
        });
        
        return potentialBindings;
    };
    
    // Generate potential bindings
    const potentialBindings = deducePotentialBindings(matchedGames, ncaaSchedulesCache, teamsData);
    
    // Research and add high-confidence potential bindings to the binding model
    if (potentialBindings.length > 0) {
        // Filter for high-confidence bindings that aren't already bound
        const highConfidenceBindings = potentialBindings.filter(binding => !binding.already_bound)
        
        if (highConfidenceBindings.length > 0 && verbose) {
            console.log(`\u001b[36mFound ${highConfidenceBindings.length} high-confidence potential bindings to research...\u001b[0m`);
            
            try {
                // Import and call the research function from teams script
                const { researchPotentialNewBindings } = await import('../teams/football-teams-college.js');
                const researchResults = await researchPotentialNewBindings(
                    highConfidenceBindings.map(b => ({
                        espnId: b.espn_id,
                        ncaaId: b.ncaa_id,
                        confidence: b.confidence
                    })),
                    verbose,
                    browsers
                );
                
                if (researchResults.successful > 0) {
                    console.log(`\u001b[32m✓ Successfully added ${researchResults.successful} new bindings to the model!\u001b[0m`);
                    console.log(`\u001b[90m  Next run will include these new team matches\u001b[0m`);
                }
            } catch (error) {
                if (verbose) console.log(`\u001b[33mWarning: Failed to research potential bindings: ${error.message}\u001b[0m`);
            }
        }
    }

    // Extract unmatched games and save to output files
    if (save) {
        try {
            // Create output directories if they don't exist
            if (!fs.existsSync('output')) {
                fs.mkdirSync('output', { recursive: true });
            }
            if (!fs.existsSync('output/csv')) {
                fs.mkdirSync('output/csv', { recursive: true });
            }
            
            // Find unmatched ESPN games (completed games without reference_id)
            const unmatchedEspnGames = matchedGames.filter(game => {
                if (!game.date_time) return false;
                const gameDate = new Date(game.date_time);
                const now = new Date();
                return gameDate < now && !game.reference_id;
            }).map(game => {
                const homeTeam = teamsData.find(t => t.espn_id === game.home_espn_id);
                const awayTeam = teamsData.find(t => t.espn_id === game.away_espn_id);
                return {
                    espn_id: game.espn_id,
                    date_time: game.date_time,
                    season: game.season,
                    title: game.title,
                    short_title: game.short_title,
                    venue: game.venue,
                    home_team: homeTeam?.short_name || 'Unknown',
                    home_espn_id: game.home_espn_id,
                    home_ncaa_id: homeTeam?.ncaa_id || null,
                    away_team: awayTeam?.short_name || 'Unknown', 
                    away_espn_id: game.away_espn_id,
                    away_ncaa_id: awayTeam?.ncaa_id || null,
                    home_score: game.home_score,
                    away_score: game.away_score,
                    winner: game.winner
                };
            });
            
            // Find unmatched NCAA games
            const allMatchedNcaaIds = new Set();
            matchedGames.forEach(game => {
                if (game.reference_id) allMatchedNcaaIds.add(game.reference_id);
            });
            
            // Flatten all NCAA games from cache
            const allNcaaGames = [];
            Object.values(ncaaSchedulesCache).forEach(teamCache => {
                if (teamCache.games && Array.isArray(teamCache.games)) {
                    allNcaaGames.push(...teamCache.games);
                }
            });
            
            // Deduplicate NCAA games and find unmatched ones
            const uniqueNcaaGames = {};
            allNcaaGames.forEach(game => {
                if (game.ncaa_game_id && !uniqueNcaaGames[game.ncaa_game_id]) {
                    uniqueNcaaGames[game.ncaa_game_id] = game;
                }
            });
            
            const unmatchedNcaaGames = Object.values(uniqueNcaaGames)
                .filter(game => !allMatchedNcaaIds.has(game.ncaa_game_id))
                .map(game => {
                    const homeTeam = teamsData.find(t => t.ncaa_id === game.home_team_ncaa_id);
                    const awayTeam = teamsData.find(t => t.ncaa_id === game.opponent_ncaa_id);
                    return {
                        ncaa_game_id: game.ncaa_game_id,
                        date: game.date,
                        season: game.season,
                        home_team: homeTeam?.short_name || 'Unknown',
                        home_team_ncaa_id: game.home_team_ncaa_id,
                        home_espn_id: homeTeam?.espn_id || null,
                        opponent_team: awayTeam?.short_name || game.opponent_name || 'Unknown',
                        opponent_ncaa_id: game.opponent_ncaa_id,
                        opponent_espn_id: awayTeam?.espn_id || null,
                        score: game.score
                    };
                });
            
            // Save unmatched games to files
            fs.writeFileSync('output/unmatched-espn-games.json', JSON.stringify(unmatchedEspnGames, null, 2));
            fs.writeFileSync('output/unmatched-ncaa-games.json', JSON.stringify(unmatchedNcaaGames, null, 2));
            
            // Generate and save potential bindings CSV
            if (potentialBindings.length > 0) {
                const csvHeader = 'espn_id,ncaa_id,sport,espn_game_title,ncaa_team_name,bound_team_name,confidence,evidence_game_date,already_bound,existing_espn_id';
                const csvRows = potentialBindings.map(binding => {
                    const espnGameDate = new Date(binding.espn_game_date).toLocaleDateString('en-US');
                    return `${binding.espn_id},${binding.ncaa_id},football,"${binding.espn_game_title}","${binding.ncaa_team_name}","${binding.bound_team_name}",${binding.confidence},${espnGameDate},${binding.already_bound},${binding.existing_espn_id || ''}`;
                });
                const csvContent = [csvHeader, ...csvRows].join('\n');
                
                fs.writeFileSync('output/csv/new-potential-binds.csv', csvContent, 'utf8');
                
                if (verbose) {
                    console.log(`\u001b[32mGenerated ${potentialBindings.length} potential ESPN->NCAA bindings in output/csv/new-potential-binds.csv\u001b[0m`);
                }
            } else if (verbose) {
                console.log(`\u001b[90mNo potential bindings found to generate\u001b[0m`);
            }
            
            if (verbose) {
                console.log(`\u001b[90mSaved ${unmatchedEspnGames.length} unmatched ESPN games to output/unmatched-espn-games.json\u001b[0m`);
                console.log(`\u001b[90mSaved ${unmatchedNcaaGames.length} unmatched NCAA games to output/unmatched-ncaa-games.json\u001b[0m`);
            }
            
        } catch (error) {
            if (verbose) console.log(`\u001b[33mWarning: Failed to save unmatched games files: ${error.message}\u001b[0m`);
        }
    }

    // Close browsers
    if (browsers.length > 0) await Promise.all(browsers.map(browser => browser.close()));

    /**
     * Generates a unique game ID using ESPN ID and game title
     * 
     * @param {string} espnId - The ESPN game ID
     * @param {string} shortTitle - The short title of the game
     * @returns {string} An 8-character unique identifier
     */
    const generateGameId = (espnId, shortTitle) => {
        if (!espnId) return null;
        // Create a consistent hash using ESPN ID as primary identifier
        const input = `CF${espnId}-${shortTitle || ''}`;
        const hash = crypto.createHash('md5').update(input).digest('hex');
        // Return first 8 characters as uppercase
        return hash.substring(0, 8).toUpperCase();
    };

    // Generate unique IDs for each game
    matchedGames.forEach(game => { game.id = generateGameId(game.espn_id, game.short_title); });
    // Save the final data
    if (save) {
        const finalJson = JSON.stringify(matchedGames, null, 2);
        fs.writeFileSync(OUTPUT_FILE, finalJson, "utf8");
        if (verbose) console.log(`\u001b[90mCollege Football Games Data Saved To: ${OUTPUT_FILE}\u001b[0m`);
    }
    return matchedGames;
}

// Export the function as default
export default get_formatted_games;

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
    get_formatted_games();
}