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

import cacheManager from './cache-manager.js';

const CONSOLIDATION_CACHE_KEY = 'ncaa_id_consolidation_map';
const CACHE_TTL = 365 * 24 * 60 * 60 * 1000; // 1 year

/**
 * Loads the entire consolidation map from the cache.
 * @returns {Object} The full, multi-sport consolidation map.
 */
const _loadMap = () => {
    return cacheManager.get(CONSOLIDATION_CACHE_KEY, CACHE_TTL) || {};
};

/**
 * Saves the entire consolidation map to the cache.
 * @param {Object} map - The full, multi-sport consolidation map to save.
 */
const _saveMap = (map) => {
    cacheManager.set(CONSOLIDATION_CACHE_KEY, map);
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
        if (team.ncaa_id) {
            const canonicalId = getCanonicalNcaaId(team.ncaa_id, sport);
            return { ...team, ncaa_id: canonicalId };
        }
        return team;
    });
};