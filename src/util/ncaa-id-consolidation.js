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
const CACHE_TTL = 365 * 24 * 60 * 60 * 1000;

/**
 * Get the canonical NCAA ID for a given NCAA ID
 * If the ID is a duplicate, returns the canonical ID it maps to
 * Otherwise returns the original ID
 * @param {string} ncaaId - NCAA ID to resolve
 * @returns {string} Canonical NCAA ID
 */
export const getCanonicalNcaaId = (ncaaId) => {
    if (!ncaaId) return ncaaId;
    const map = cacheManager.get(CONSOLIDATION_CACHE_KEY, CACHE_TTL) || {};
    return map[ncaaId] || ncaaId;
};

/**
 * Add a duplicate NCAA ID mapping to the consolidation map
 * @param {string} duplicateNcaaId - The duplicate NCAA ID
 * @param {string} canonicalNcaaId - The canonical NCAA ID it should map to
 * @param {boolean} save - Whether to save the map immediately (default: true)
 * @returns {Object} Updated consolidation map
 */
export const addDuplicateMapping = (duplicateNcaaId, canonicalNcaaId, save = true) => {
    const consolidationMap = cacheManager.get(CONSOLIDATION_CACHE_KEY, CACHE_TTL) || {};
    // Prevent circular mappings - ensure canonical ID is not itself a duplicate
    const resolvedCanonical = getCanonicalNcaaId(canonicalNcaaId);
    consolidationMap[duplicateNcaaId] = resolvedCanonical;
    if (save) cacheManager.set(CONSOLIDATION_CACHE_KEY, consolidationMap);
    return consolidationMap;
};


/**
 * Apply consolidation to a list of games, replacing duplicate NCAA IDs with canonical ones
 * @param {Array} games - Array of game objects
 * @param {Array<string>} ncaaIdFields - Fields in game objects that contain NCAA IDs
 * @returns {Array} Games with NCAA IDs consolidated
 */
export const consolidateGamesNcaaIds = (games, ncaaIdFields = ['home_team_ncaa_id', 'opponent_ncaa_id']) => {
    return games.map(game => {
        const consolidatedGame = { ...game };
        ncaaIdFields.forEach(field => {
            if (consolidatedGame[field]) consolidatedGame[field] = getCanonicalNcaaId(consolidatedGame[field]);
        });
        return consolidatedGame;
    });
};

/**
 * Consolidate NCAA IDs in teams data
 * @param {Array} teamsData - Array of team objects with ncaa_id field
 * @returns {Array} Teams data with NCAA IDs consolidated
 */
export const consolidateTeamsNcaaIds = (teamsData) => {
    return teamsData.map(team => {
        if (team.ncaa_id) {
            const canonicalId = getCanonicalNcaaId(team.ncaa_id);
            return { ...team, ncaa_id: canonicalId };
        }
        return team;
    });
};