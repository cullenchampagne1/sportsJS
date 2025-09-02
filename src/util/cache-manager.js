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
import path from 'path';

/**
 * A robust file-based cache manager with TTL and error handling.
 * This class provides a simple way to cache data to the filesystem, with
 * support for time-to-live (TTL) to automatically expire cache entries.
 */
class CacheManager {
   
    /**
     * Creates an instance of the CacheManager.
     * 
     * @param {object} options - Configuration options for the cache manager.
     * @param {string} options.cacheDir - The directory where cache files will be stored.
     * @param {number} [options.defaultTtl=3600000] - Default time-to-live in milliseconds for cache entries (defaults to 1 hour).
     */
    constructor({ cacheDir, defaultTtl = 3600000 }) {
        if (!cacheDir) throw new Error('Cache directory must be provided.'); 
        this.cacheDir = path.resolve(cacheDir);
        this.defaultTtl = defaultTtl;
        this.ensureCacheDirExists();
    }

    /**
     * Ensures that the cache directory exists, creating it if necessary.
     * @private
     */
    ensureCacheDirExists = () => fs.mkdirSync(this.cacheDir, { recursive: true });

    /**
     * Generates the full file path for a given cache key.
     * It sanitizes the key to make it safe for use as a filename.
     * 
     * @param {string} key - The unique identifier for the cache entry.
     * @returns {string} The absolute path to the cache file.
     * @private
     */
    getCachePath(key) {
        const sanitizedKey = key.replace(/[^a-z0-9_.-]/gi, '_');
        return path.join(this.cacheDir, `${sanitizedKey}.json`);
    }

    /**
     * Checks if a cache entry for a given key is still valid based on its TTL.
     * 
     * @param {string} key - The key of the cache entry to validate.
     * @param {number} [ttl=this.defaultTtl] - The TTL for this specific check, in milliseconds. Defaults to the instance's defaultTtl.
     * @returns {boolean} True if the cache is valid, false otherwise.
     */
    isCacheValid(key, ttl = this.defaultTtl) {
        const cacheFile = this.getCachePath(key);
        try {
            const stats = fs.statSync(cacheFile);
            const fileAge = Date.now() - stats.mtime.getTime();
            return fileAge < ttl;
        } catch (error) {
            return false; // File doesn't exist or other error
        }
    }

    /**
     * Retrieves an item from the cache if it exists and is not expired.
     * 
     * @param {string} key - The key of the item to retrieve.
     * @param {number} [ttl=this.defaultTtl] - The TTL for this specific retrieval, in milliseconds.
     * @returns {{data: any, savedAt: Date}|null} An object containing the cached data and the timestamp it was saved, or null if the cache is invalid or an error occurs.
     */
    get(key, ttl = this.defaultTtl) {
        const cacheFile = this.getCachePath(key);
        if (!this.isCacheValid(key, ttl)) {
            this.invalidate(key); // Remove expired file
            return null;
        }
        try {
            const stats = fs.statSync(cacheFile);
            const data = fs.readFileSync(cacheFile, 'utf8');
            return {
                data: JSON.parse(data),
                savedAt: new Date(stats.mtime.getTime())
            };
        } catch (error) {
            // If parsing fails, invalidate the corrupt cache file
            this.invalidate(key);
            return null;
        }
    }

    /**
     * Saves an item to the cache.
     * 
     * @param {string} key - The key under which to store the data.
     * @param {any} data - The data to be cached. It should be serializable to JSON.
     */
    set(key, data) {
        const cacheFile = this.getCachePath(key);
        try {
            fs.writeFileSync(cacheFile, JSON.stringify(data, null, 2), 'utf8');
        } catch (error) {
            console.error(`Failed to save cache for key "${key}":`, error);
        }
    }

    /**
     * Deletes a specific cache entry by its key.
     * 
     * @param {string} key - The key of the cache entry to delete.
     * @returns {boolean} True if the entry was successfully deleted, false otherwise.
     */
    invalidate(key) {
        const cacheFile = this.getCachePath(key);
        try {
            if (fs.existsSync(cacheFile)) {
                fs.unlinkSync(cacheFile);
                return true;
            }
        } catch (error) {
            console.error(`Failed to invalidate cache for key "${key}":`, error);
        }
        return false;
    }

    /**
     * Clears the entire cache directory, removing all cache files.
     */
    clearAll() {
        try {
            fs.rmSync(this.cacheDir, { recursive: true, force: true });
            this.ensureCacheDirExists();
        } catch (error) {
            console.error(`Failed to clear the cache directory:`, error);
        }
    }
}

// Create the singleton instance of the CacheManager.
// This instance is configured to use the './data/cache' directory and a default
// TTL of 24 hours (86,400,000 milliseconds).
const cacheManagerInstance = new CacheManager({
    cacheDir: './data/raw',
    defaultTtl: 86400000 // 24 hours
});

export default cacheManagerInstance;
