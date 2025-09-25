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
import Fuse from 'fuse.js';
import crypto from 'crypto';
import { getCanonicalNcaaId } from './ncaa-id-consolidation.js';

const SOURCES = {
    basketball: {
        endpoint: "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=200000",
        bindingFile: "data/models/basketball-espn-ncaa-binding.json"
    },
    basketballW: {
        endpoint: "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams?limit=200000",
        bindingFile: "data/models/basketball-w-espn-ncaa-binding.json"
    },
    football: {
        endpoint: "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams?limit=200000",
        bindingFile: "data/models/football-espn-ncaa-binding.json"
    },
    soccerW: {
        endpoint: "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.ncaa.w.1/teams?limit=200000",
        bindingFile: "data/models/soccer-w-espn-ncaa-binding.json"
    },
    soccer: {
        endpoint: "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.ncaa.1/teams?limit=200000",
        bindingFile: "data/models/soccer-espn-ncaa-binding.json"
    }
};

const commonNicknames = new Set(['eagles', 'tigers', 'bulldogs', 'wildcats', 'panthers', 'lions', 'bears', 'cardinals', 'hawks', 'knights', 'trojans', 'spartans', 'warriors', 'vikings', 'pirates', 'flames', 'saints', 'demons', 'rebels', 'mustangs', 'cougars', 'rams', 'wolves', 'falcons', 'jaguars', 'bison', 'broncos', 'colts', 'hornets', 'owls', 'bobcats', 'raccoons']);
const stateMappings = {
    'al': 'alabama', 'ak': 'alaska', 'az': 'arizona', 'ar': 'arkansas', 'ca': 'california',
    'co': 'colorado', 'ct': 'connecticut', 'de': 'delaware', 'fl': 'florida', 'ga': 'georgia',
    'hi': 'hawaii', 'id': 'idaho', 'il': 'illinois', 'in': 'indiana', 'ia': 'iowa',
    'ks': 'kansas', 'ky': 'kentucky', 'la': 'louisiana', 'me': 'maine', 'md': 'maryland',
    'ma': 'massachusetts', 'mi': 'michigan', 'mn': 'minnesota', 'ms': 'mississippi', 'mo': 'missouri',
    'mt': 'montana', 'ne': 'nebraska', 'nv': 'nevada', 'nh': 'new hampshire', 'nj': 'new jersey',
    'nm': 'new mexico', 'ny': 'new york', 'nc': 'north carolina', 'nd': 'north dakota', 'oh': 'ohio',
    'ok': 'oklahoma', 'or': 'oregon', 'pa': 'pennsylvania', 'ri': 'rhode island', 'sc': 'south carolina',
    'sd': 'south dakota', 'tn': 'tennessee', 'tx': 'texas', 'ut': 'utah', 'vt': 'vermont',
    'va': 'virginia', 'wa': 'washington', 'wv': 'west virginia', 'wi': 'wisconsin', 'wy': 'wyoming'
};

/**
 * Generates a deterministic ID with an optional abbreviation prefix and a hash suffix.
 * @param {string} input - Any unique input (e.g., ESPN ID, team name) to be hashed.
 * @param {string} [abbreviation=""] - Optional abbreviation to use as a prefix (2-3 chars).
 * @param {number} [length=8] - The total desired length of the final ID.
 * @returns {string} A deterministic ID of the specified length.
 */
function generateId(input, abbreviation = "", length = 8) {
    if (length < 4) throw new Error("Length must be at least 4 (to fit prefix + hash)");
    let prefix = abbreviation ? abbreviation.slice(0, 3) : "";
    if (prefix.length < 2) prefix = prefix.padEnd(2, "x");
    const hash = crypto.createHash("sha256").update(String(input)).digest("hex");
    const availableForHash = length - prefix.length;
    const hashPart = hash.slice(0, availableForHash);
    return (prefix + hashPart).toUpperCase();
}

/**
 * Normalizes a team name by performing a series of text transformations.
 *
 * @param {string} name - The name to be cleaned.
 * @returns {string} The normalized and trimmed team name.
 */
const cleanName = (name) => {
    if (!name) return '';
    let processed = name.toLowerCase()
        .replace(/'/g, '')
        .replace(/&/g, 'and')
        .replace(/[.()]/g, (match) => {
            const content = match.replace(/[.()]/g, '');
            return stateMappings[content] ? ` ${stateMappings[content]}` : '';
        })
        .replace(/-/g, ' ')
        .replace(/\b(st|state)\b/g, 'state')
        .replace(/\bsaint\b/g, 'st')
        .replace(/\b(mount|mt)\b/g, 'mount')
        .replace(/\b(california state|cal state)\b/g, 'csu')
        .replace(/\b(southern illinois)\b/g, 'siu')
        .replace(/\b(suny|state university of new york)\b/g, 'suny')
        .replace(/\b(university of|university|univ|college)\b/g, '')
        .replace(/\bat\b/g, '');
    const words = processed.split(' ').filter(w => !commonNicknames.has(w));
    return words.join(' ').replace(/\s+/g, ' ').trim();
};

/**
 * Generates an acronym from a given string by taking the first letter of each word.
 * It ignores common "stop words" like "of," "the," and "and."
 *
 * @param {string} name - The full name or phrase to create an acronym from.
 * @returns {string} The generated acronym in lowercase.
 */
const generateAcronym = (name) => {
    if (!name) return '';
    const stopWords = new Set(['of', 'the', 'and', 'at', 'in', 'a', 'an', 'for', 'to']);
    return name.toLowerCase().split(/[\s-]+/).filter(w => w && !stopWords.has(w)).map(w => w[0]).join('');
};

/**
 * Performs fuzzy matching using Fuse.js with enhanced pre-processing to find likely matches.
 * 
 * @param {Array<Object>} unmatchedEspn - Array of unmatched ESPN teams.
 * @param {Array<Object>} unmatchedNcaa - Array of unmatched NCAA teams.
 * @param {boolean} verbose - If true, enables detailed logging.
 * @returns {{newMatches: Array<Object>, remainingEspn: Array<Object>, remainingNcaa: Array<Object>}} The results of the matching operation.
 */
const _performFuzzyMatching = (unmatchedEspn, unmatchedNcaa, verbose) => {
    verbose && console.log(`\u001b[36mPerforming fuzzy matching: ${unmatchedEspn.length} ESPN vs ${unmatchedNcaa.length} NCAA\u001b[0m`);
    const ncaaFuse = new Fuse(unmatchedNcaa.map(team => ({ ...team, processedName: cleanName(team.school_name || team.name_ncaa), generatedAbv: generateAcronym(team.school_name || team.name_ncaa) })), {
        keys: [{ name: 'processedName', weight: 0.7 }, { name: 'generatedAbv', weight: 0.3 }],
        threshold: 0.4,
        includeScore: true,
    });
    const potentialMatches = unmatchedEspn.reduce((acc, espnTeam) => {
        const results = ncaaFuse.search(cleanName(`${espnTeam.displayName} ${espnTeam.shortDisplayName} ${espnTeam.location}`));
        if (results[0]?.score < 0.35) acc.push({ espnTeam, ncaaTeam: results[0].item, score: results[0].score });
        return acc;
    }, []).sort((a, b) => a.score - b.score);
    const newMatches = [];
    const [usedEspnIds, usedNcaaIds] = [new Set(), new Set()];
    potentialMatches.forEach(match => {
        if (!usedEspnIds.has(match.espnTeam.id) && !usedNcaaIds.has(match.ncaaTeam.ncaa_id)) {
            newMatches.push({ espnTeam: match.espnTeam, ncaaTeam: match.ncaaTeam });
            usedEspnIds.add(match.espnTeam.id);
            usedNcaaIds.add(match.ncaaTeam.ncaa_id);
        }
    });
    const remainingEspn = unmatchedEspn.filter(t => !usedEspnIds.has(t.id));
    const remainingNcaa = unmatchedNcaa.filter(t => !usedNcaaIds.has(t.ncaa_id));
    verbose && console.log(`\u001b[32mFuzzy matching complete: Found ${newMatches.length} new matches.\u001b[0m`);
    return { newMatches, remainingEspn, remainingNcaa };
};

/**
 * Leverages existing confirmed bindings from other sports to find matches for teams at the same schools.
 * 
 * @param {Array<Object>} unmatchedEspnTeams - Unmatched ESPN team objects.
 * @param {Array<Object>} unmatchedNcaaTeams - Unmatched NCAA team objects.
 * @param {boolean} verbose - Enable detailed logging.
 * @returns {Promise<{newMatches: Array<Object>, remainingEspn: Array<Object>, remainingNcaa: Array<Object>}>} The results of the cross-sport matching.
 */
const _leverageExistingBindings = async (unmatchedEspnTeams, unmatchedNcaaTeams, verbose) => {
    const log = (m, c = 36) => verbose && console.log(`\u001b[${c}m${m}\u001b[0m`);
    const [newMatches, usedEspn, usedNcaa, cross, espnData, schoolMap] = [[], new Set(), new Set(), new Map(), new Map(), new Map()];
    for (const [sport, { bindingFile }] of Object.entries(SOURCES)) {
        try {
            if (fs.existsSync(bindingFile)) Object.entries(JSON.parse(fs.readFileSync(bindingFile, "utf8"))).forEach(([k, v]) => cross.set(`${k}-${v}`, { ncaaId: k, espnId: v, sport }));
        } catch (e) { log(`Warn: ${bindingFile} ${e.message}`, 33); }
    }
    await Promise.all(Object.entries(SOURCES).map(async ([sport, { endpoint }]) => {
        try {
            const data = await fetch(endpoint).then(r => r.ok ? r.json() : null);
            (data?.sports?.[0]?.leagues?.[0]?.teams || []).forEach(t => espnData.set(String(t.team.id), { ...t.team, sport }));
        } catch (e) { log(`Warn: fetch ${sport} ${e.message}`, 33); }
    }));
    for (const { ncaaId, espnId, sport } of cross.values()) {
        const t = espnData.get(espnId);
        if (t) [t.location, cleanName(t.displayName), t.name].map(k => k?.toLowerCase().trim()).filter(k => k?.length >= 3).forEach(key => {
            const d = schoolMap.get(key) || { espnIds: new Set(), ncaaIds: new Set(), sports: new Set() };
            d.espnIds.add(espnId); d.ncaaIds.add(ncaaId); d.sports.add(sport);
            schoolMap.set(key, d);
        });
    }
    log(`Built NCAA->ESPN School map with ${schoolMap.size} existing keys`);
    const isSafeMatch = (name1, name2) => {
        if (!name1 || !name2) return false;
        const [clean1, clean2] = [cleanName(name1), cleanName(name2)];
        if (clean1 === clean2) return true;
        const [words1, words2] = [new Set(clean1.split(' ')), new Set(clean2.split(' '))];
        const intersectionSize = [...words1].filter(x => words2.has(x)).length;
        return intersectionSize / Math.min(words1.size, words2.size) > 0.8;
    };
    unmatchedEspnTeams.forEach(eTeam => {
        if (usedEspn.has(String(eTeam.id))) return;
        const match = unmatchedNcaaTeams.find(n => !usedNcaa.has(n.ncaa_id) && [eTeam.displayName, eTeam.location, eTeam.name].filter(Boolean).some(k => [n.school_name, n.name_ncaa, n.university].filter(Boolean).some(nk => isSafeMatch(k, nk))));
        if (match) {
            newMatches.push({ espnTeam: eTeam, ncaaTeam: match });
            usedEspn.add(String(eTeam.id));
            usedNcaa.add(match.ncaa_id);
        }
    });
    const remainingEspn = unmatchedEspnTeams.filter(t => !usedEspn.has(String(t.id)));
    const remainingNcaa = unmatchedNcaaTeams.filter(t => !usedNcaa.has(t.ncaa_id));
    log(`Cross-sport binding: ${newMatches.length} matches, ${remainingEspn.length} ESPN left, ${remainingNcaa.length} NCAA left`, 32);
    return { newMatches, remainingEspn, remainingNcaa };
};

/**
 * Comprehensive ESPN-NCAA team matching function that uses a deterministic, multi-stage local algorithm.
 * @param {Array<Object>} espnTeams - Array of ESPN team objects.
 * @param {Array<Object>} ncaaTeamsWithIds - Array of NCAA team objects with IDs.
 * @param {Object} bindings - Existing espn->ncaa bindings available.
 * @param {Function} formatTeamFn - Function to format matched team objects.
 * @param {boolean} verbose - If true, enables detailed logging.
 * @returns {Promise<{matchedTeams: Array<Object>, unmatchedEspn: Array<Object>, unmatchedNcaa: Array<Object>, updatedBindings: Object}>} Complete matching results.
 */
const matchEspnToNcaaTeams = async (espnTeams, ncaaTeamsWithIds, bindings, formatTeamFn, verbose) => {

    // STAGE 1: Primary matching using existing bindings
    const primaryMatchedData = [];
    const usedEspnIds = new Set();
    const usedNcaaIds = new Set();

    // Create a map of ESPN teams for quick lookup
    const espnTeamMap = new Map(espnTeams.map(team => [String(team.id), team]));

    // Iterate through NCAA teams and try to match them using existing bindings
    // Note: NCAA teams already have canonical IDs from consolidation applied earlier
    ncaaTeamsWithIds.forEach(ncaaTeam => {
        const ncaaId = String(ncaaTeam.ncaa_id); // Use as-is since consolidation already applied
        if (bindings[ncaaId]) { // Check if this NCAA ID has a binding
            const espnId = String(bindings[ncaaId]);
            const espnTeam = espnTeamMap.get(espnId);

            if (espnTeam && !usedEspnIds.has(espnId) && !usedNcaaIds.has(ncaaId)) {
                primaryMatchedData.push({ ...espnTeam, ...ncaaTeam });
                usedEspnIds.add(espnId);
                usedNcaaIds.add(ncaaId);
            }
        }
    });

    let unmatchedEspn = espnTeams.filter(team => !usedEspnIds.has(String(team.id)));
    let unmatchedNcaa = ncaaTeamsWithIds.filter(team => !usedNcaaIds.has(String(team.ncaa_id)));

    // STAGE 2: Cross-sport binding leverage
    const crossSportResults = await _leverageExistingBindings(unmatchedEspn, unmatchedNcaa, verbose);
    const crossSportMatchedData = crossSportResults.newMatches.map(match => ({ ...match.espnTeam, ...match.ncaaTeam }));
    unmatchedEspn = crossSportResults.remainingEspn;
    unmatchedNcaa = crossSportResults.remainingNcaa;
    // STAGE 3: Fuzzy matching for the remaining teams
    const fuzzyResults = _performFuzzyMatching(unmatchedEspn, unmatchedNcaa, verbose);
    const fuzzyMatchedData = fuzzyResults.newMatches.map(match => ({ ...match.espnTeam, ...match.ncaaTeam }));
    unmatchedEspn = fuzzyResults.remainingEspn;
    unmatchedNcaa = fuzzyResults.remainingNcaa;
    // STAGE 4: Final combination and formatting
    const allMatchedData = [...primaryMatchedData, ...crossSportMatchedData, ...fuzzyMatchedData];
    const formattedTeams = allMatchedData.map(team => formatTeamFn(team));

    // Update bindings with all matches found
    const updatedBindings = { ...bindings };
    allMatchedData.forEach(team => {
        if (team.ncaa_id && team.id) {
            // Use NCAA ID as-is since it's already canonical from earlier consolidation
            updatedBindings[String(team.ncaa_id)] = String(team.id);
        }
    });
    verbose && console.log(`\u001b[32mTotal ESPN-NCAA matching complete: ${formattedTeams.length} teams matched, ${unmatchedEspn.length} ESPN unmatched, ${unmatchedNcaa.length} NCAA unmatched\u001b[0m`);
    return {
        matchedTeams: formattedTeams,
        unmatchedEspn: unmatchedEspn,
        unmatchedNcaa: unmatchedNcaa,
        updatedBindings: updatedBindings
    };
};

export { matchEspnToNcaaTeams, generateId };