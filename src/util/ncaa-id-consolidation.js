/**
 * @fileoverview Manages the consolidation of duplicate NCAA team IDs to a single canonical ID, now on a per-sport basis.
 * @version 2.0.0
 */

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

import fs from 'fs';

const CONSOLIDATION_FILE_PATH = 'data/raw/ncaa_id_consolidation_map.json';

/**
 * Loads the entire consolidation map from the file system.
 * @returns {Object} The full, multi-sport consolidation map.
 */
const _loadMap = () => {
    try {
        if (fs.existsSync(CONSOLIDATION_FILE_PATH)) {
            const rawData = fs.readFileSync(CONSOLIDATION_FILE_PATH, 'utf8');
            if (rawData) {
                return JSON.parse(rawData);
            }
        }
    } catch (error) {
        console.error(`Error loading consolidation map from ${CONSOLIDATION_FILE_PATH}: ${error}`);
    }
    return {};
};

/**
 * Saves the entire consolidation map to the file system.
 * @param {Object} map - The full, multi-sport consolidation map to save.
 */
const _saveMap = (map) => {
    try {
        fs.writeFileSync(CONSOLIDATION_FILE_PATH, JSON.stringify(map, null, 2), 'utf8');
    } catch (error) {
        console.error(`Error saving consolidation map to ${CONSOLIDATION_FILE_PATH}: ${error}`);
    }
};

/**
 * Gets the canonical NCAA ID for a given NCAA ID within a specific sport.
 * If the ID is a duplicate for that sport, returns the canonical ID it maps to.
 * Otherwise, returns the original ID.
 * @param {string} ncaaId - NCAA ID to resolve.
 * @param {string} sport - The sport context (e.g., 'football', 'basketball').
 * @returns {string} Canonical NCAA ID for that sport.
 */
export const getCanonicalNcaaId = (ncaaId, sport) => {
    if (!ncaaId || !sport) return ncaaId;
    const mainMap = _loadMap();
    const sportMap = mainMap[sport] || {};
    return sportMap[ncaaId] || ncaaId;
};

/**
 * Adds a duplicate NCAA ID mapping to the consolidation map for a specific sport.
 * @param {string} duplicateNcaaId - The duplicate NCAA ID.
 * @param {string} canonicalNcaaId - The canonical NCAA ID it should map to.
 * @param {string} sport - The sport context (e.g., 'football', 'basketball').
 * @param {boolean} [save=true] - Whether to save the map immediately.
 * @returns {Object} The updated sport-specific consolidation map.
 */
export const addDuplicateMapping = (duplicateNcaaId, canonicalNcaaId, sport, save = true) => {
    if (!sport) throw new Error("Sport context is required for duplicate mapping.");
    const mainMap = _loadMap();
    if (!mainMap[sport]) {
        mainMap[sport] = {};
    }

    // Prevent circular mappings - ensure canonical ID is not itself a duplicate within the same sport
    const resolvedCanonical = getCanonicalNcaaId(canonicalNcaaId, sport);
    mainMap[sport][duplicateNcaaId] = resolvedCanonical;

    if (save) {
        _saveMap(mainMap);
    }
    return mainMap[sport];
};

/**
 * Applies consolidation to a list of games for a specific sport.
 * @param {Array<Object>} games - Array of game objects.
 * @param {string} sport - The sport context.
 * @param {Array<string>} [ncaaIdFields] - Fields in game objects that contain NCAA IDs.
 * @returns {Array<Object>} Games with NCAA IDs consolidated for that sport.
 */
export const consolidateGamesNcaaIds = (games, sport, ncaaIdFields = ['home_team_ncaa_id', 'opponent_ncaa_id']) => {
    if (!sport) return games;
    return games.map(game => {
        const consolidatedGame = { ...game };
        ncaaIdFields.forEach(field => {
            if (consolidatedGame[field]) {
                consolidatedGame[field] = getCanonicalNcaaId(consolidatedGame[field], sport);
            }
        });
        return consolidatedGame;
    });
};

/**
 * Consolidates NCAA IDs in teams data for a specific sport.
 * @param {Array<Object>} teamsData - Array of team objects with ncaa_id field.
 * @param {string} sport - The sport context.
 * @returns {Array<Object>} Teams data with NCAA IDs consolidated for that sport.
 */
export const consolidateTeamsNcaaIds = (teamsData, sport) => {
    if (!sport) return teamsData;
    return teamsData.map(team => {
        if (team.reference_id) {
            const canonicalId = getCanonicalNcaaId(team.reference_id, sport);
            return { ...team, reference_id: canonicalId };
        }
        return team;
    });
};

/**
 * Attempts to find a team in the teams data by matching team names.
 * @param {string} teamName - The team name from ESPN schedule data.
 * @param {Array<Object>} teamsData - Array of team objects to search.
 * @returns {Object|null} The matching team object or null if no match found.
 */
export const findTeamByName = (teamName, teamsData) => {
    if (!teamName || !teamsData?.length) return null;
    
    const cleanName = (name) => name?.toLowerCase().replace(/[^\w\s]/g, '').trim();
    const searchName = cleanName(teamName);
    
    // Try exact matches first
    let match = teamsData.find(team => 
        cleanName(team.short_name) === searchName ||
        cleanName(team.full_name) === searchName ||
        cleanName(team.university) === searchName
    );
    
    if (match) return match;
    
    // Try partial matches using key words
    const searchWords = searchName.split(/\s+/).filter(word => word.length > 2);
    
    match = teamsData.find(team => {
        const teamWords = [
            ...cleanName(team.short_name || '').split(/\s+/),
            ...cleanName(team.full_name || '').split(/\s+/),
            ...cleanName(team.university || '').split(/\s+/)
        ].filter(word => word.length > 2);
        
        // Check if most search words are found in team data
        const matchingWords = searchWords.filter(word => 
            teamWords.some(teamWord => teamWord.includes(word) || word.includes(teamWord))
        );
        
        return matchingWords.length >= Math.min(searchWords.length, 2);
    });
    
    return match || null;
};

/**
 * Retrieves a cache entry by its canonical NCAA ID, handling cases where the cache might contain
 * entries keyed by non-canonical (older) IDs. If a non-canonical key maps to the desired canonical ID,
 * the entry is returned. This function does NOT modify the cache to update keys.
 * 
 * @param {Object} cache - The cache object (e.g., ncaaSchedulesCache.data).
 * @param {string} ncaaId - The NCAA ID to look up (can be canonical or non-canonical).
 * @param {string} sport - The sport context (e.g., 'football').
 * @returns {Object|null} The cache entry if found, otherwise null.
 */
export const getCacheEntryByCanonicalId = (cache, ncaaId, sport) => {
    if (!cache || !ncaaId || !sport) return null;
    const canonicalId = getCanonicalNcaaId(ncaaId, sport);

    // First, try direct lookup with the canonical ID
    if (cache[canonicalId]) {
        return cache[canonicalId];
    }

    // If not found directly, iterate through cache keys to find a match
    // This handles cases where the cache might contain old, non-canonical keys
    for (const key in cache) {
        if (getCanonicalNcaaId(key, sport) === canonicalId) {
            // Found a match under a non-canonical key.
            // Note: This function does not modify the cache to update the key.
            // The caller should handle cache updates if desired.
            return cache[key];
        }
    }
    return null;
};