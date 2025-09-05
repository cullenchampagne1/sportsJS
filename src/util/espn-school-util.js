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

/**
 * @const {object} SOURCES
 * @description Configuration for cross-sport binding leverage, pointing to existing binding files.
 */
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

const generateAcronym = (name) => {
    if (!name) return '';
    const stopWords = new Set(['of', 'the', 'and', 'at', 'in', 'a', 'an', 'for', 'to']);
    return name.toLowerCase().split(/[\s-]+/).filter(w => w && !stopWords.has(w)).map(w => w[0]).join('');
};

/**
 * Performs fuzzy matching using Fuse.js with enhanced pre-processing to find likely matches.
 * @param {Array<Object>} unmatchedEspn - Array of unmatched ESPN teams.
 * @param {Array<Object>} unmatchedNcaa - Array of unmatched NCAA teams.
 * @param {boolean} verbose - If true, enables detailed logging.
 * @returns {{newMatches: Array<Object>, remainingEspn: Array<Object>, remainingNcaa: Array<Object>}} The results of the matching operation.
 */
const _performFuzzyMatching = (unmatchedEspn, unmatchedNcaa, verbose) => {
    if (verbose) console.log(`\u001b[36mPerforming fuzzy matching with Fuse.js: ${unmatchedEspn.length} ESPN vs ${unmatchedNcaa.length} NCAA\u001b[0m`);

    const processedNcaa = unmatchedNcaa.map(team => {
        const fullName = team.school_name || team.name_ncaa;
        return {
            ...team,
            processedName: cleanName(fullName),
            generatedAbv: generateAcronym(fullName)
        };
    });

    const ncaaFuse = new Fuse(processedNcaa, {
        keys: [
            { name: 'processedName', weight: 0.7 },
            { name: 'generatedAbv', weight: 0.3 }
        ],
        threshold: 0.4,
        includeScore: true,
    });

    const potentialMatches = [];
    unmatchedEspn.forEach(espnTeam => {
        const query = cleanName(`${espnTeam.displayName} ${espnTeam.shortDisplayName} ${espnTeam.location}`);
        const results = ncaaFuse.search(query);
        if (results.length > 0) {
            const bestMatch = results[0];
            if (bestMatch.score < 0.35) {
                potentialMatches.push({ espnTeam, ncaaTeam: bestMatch.item, score: bestMatch.score });
            }
        }
    });

    potentialMatches.sort((a, b) => a.score - b.score);

    const newMatches = [];
    const usedEspnIds = new Set();
    const usedNcaaIds = new Set();

    potentialMatches.forEach(match => {
        if (!usedEspnIds.has(match.espnTeam.id) && !usedNcaaIds.has(match.ncaaTeam.ncaa_id)) {
            newMatches.push({ espnTeam: match.espnTeam, ncaaTeam: match.ncaaTeam });
            usedEspnIds.add(match.espnTeam.id);
            usedNcaaIds.add(match.ncaaTeam.ncaa_id);
        }
    });

    const remainingEspn = unmatchedEspn.filter(t => !usedEspnIds.has(t.id));
    const remainingNcaa = unmatchedNcaa.filter(t => !usedNcaaIds.has(t.ncaa_id));

    if (verbose) console.log(`\u001b[32mFuzzy matching complete: Found ${newMatches.length} new matches.\u001b[0m`);

    return { newMatches, remainingEspn, remainingNcaa };
};

/**
 * Leverages existing confirmed bindings from other sports to find matches for teams at the same schools.
 * @param {Array<Object>} unmatchedEspnTeams - Unmatched ESPN team objects.
 * @param {Array<Object>} unmatchedNcaaTeams - Unmatched NCAA team objects.
 * @param {boolean} verbose - Enable detailed logging.
 * @returns {Promise<{newMatches: Array<Object>, remainingEspn: Array<Object>, remainingNcaa: Array<Object>}>} The results of the cross-sport matching.
 */
const _leverageExistingBindings = async (unmatchedEspnTeams, unmatchedNcaaTeams, verbose) => {
    const log = (m, c = 36) => verbose && console.log(`\u001b[${c}m${m}\u001b[0m`);
    const newMatches = [], usedEspn = new Set(), usedNcaa = new Set(), cross = new Map(), espnData = new Map(), schoolMap = new Map();
    
    for (const [sport, { bindingFile }] of Object.entries(SOURCES)) {
        if (!fs.existsSync(bindingFile)) continue;
        try {
            const bindings = JSON.parse(fs.readFileSync(bindingFile, "utf8"));
            for (const [ncaaId, espnId] of Object.entries(bindings)) cross.set(`${ncaaId}-${espnId}`, { ncaaId, espnId, sport });
        } catch (e) { log(`Warn: ${bindingFile} ${e.message}`, 33); }
    }

    for (const [sport, { endpoint }] of Object.entries(SOURCES)) {
        try {
            const data = await fetch(endpoint).then(r => r.ok ? r.json() : null);
            const teams = data?.sports?.[0]?.leagues?.[0]?.teams?.map(x => x.team) || [];
            teams.forEach(t => espnData.set(String(t.id), { ...t, sport }));
        } catch (e) { log(`Warn: fetch ${sport} ${e.message}`, 33); }
    }

    for (const { ncaaId, espnId, sport } of cross.values()) {
        const t = espnData.get(espnId); if (!t) continue;
        [t.location, cleanName(t.displayName), t.name].map(k => k?.toLowerCase().trim()).filter(k => k?.length >= 3)
            .forEach(key => {
                if (!schoolMap.has(key)) schoolMap.set(key, { espnIds: new Set(), ncaaIds: new Set(), sports: new Set() });
                const d = schoolMap.get(key); d.espnIds.add(espnId); d.ncaaIds.add(ncaaId); d.sports.add(sport);
            });
    }
    log(`Built NCAA->ESPN School map with ${schoolMap.size} existing keys from alternate bindings`);

    const isSafeMatch = (name1, name2) => {
        if (!name1 || !name2) return false;
        const clean1 = cleanName(name1);
        const clean2 = cleanName(name2);
        if (clean1 === clean2) return true;
        const words1 = new Set(clean1.split(' '));
        const words2 = new Set(clean2.split(' '));
        const intersection = new Set([...words1].filter(x => words2.has(x)));
        return intersection.size / Math.min(words1.size, words2.size) > 0.8;
    };

    for (const eTeam of unmatchedEspnTeams) {
        if (usedEspn.has(String(eTeam.id))) continue;
        const eKeys = [eTeam.displayName, eTeam.location, eTeam.name].filter(Boolean);
        const match = unmatchedNcaaTeams.find(n => 
            !usedNcaa.has(n.ncaa_id) && 
            eKeys.some(k => 
                [n.school_name, n.name_ncaa, n.university].filter(Boolean)
                .some(nk => isSafeMatch(k, nk))
            )
        );
        if (match) { 
            newMatches.push({ espnTeam: eTeam, ncaaTeam: match }); 
            usedEspn.add(String(eTeam.id)); 
            usedNcaa.add(match.ncaa_id); 
        }
    }

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
    const boundEspnIds = new Set(Object.values(bindings).map(String));
    const boundNcaaIds = new Set(Object.keys(bindings));

    const primaryMatchedData = [];
    Object.entries(bindings).forEach(([ncaaId, espnId]) => {
        const espnTeam = espnTeams.find(team => String(team.id) === String(espnId));
        const ncaaTeam = ncaaTeamsWithIds.find(team => String(team.ncaa_id) === ncaaId);
        if (espnTeam && ncaaTeam) {
            primaryMatchedData.push({ ...espnTeam, ...ncaaTeam });
        }
    });

    let unmatchedEspn = espnTeams.filter(team => !boundEspnIds.has(String(team.id)));
    let unmatchedNcaa = ncaaTeamsWithIds.filter(team => !boundNcaaIds.has(String(team.ncaa_id)));

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
            updatedBindings[team.ncaa_id] = String(team.id);
        }
    });

    if (verbose) {
        const totalMatched = formattedTeams.length;
        console.log(`\u001b[32mTotal ESPN-NCAA matching complete: ${totalMatched} teams matched, ${unmatchedEspn.length} ESPN unmatched, ${unmatchedNcaa.length} NCAA unmatched\u001b[0m`);
    }

    return {
        matchedTeams: formattedTeams,
        unmatchedEspn: unmatchedEspn,
        unmatchedNcaa: unmatchedNcaa,
        updatedBindings: updatedBindings
    };
};

export { matchEspnToNcaaTeams, generateId };