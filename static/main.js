/**
 * MergeWhisper — Universal URL Sanitizer & Dynamic Link Utilities
 * Handles sanitization and robust URL encoding for branches and repositories
 * used in external links (GitHub Compare, issue tracker webhooks, etc.).
 */

/**
 * Universal Branch Sanitizer
 * - Converts whitespace to hyphens
 * - Removes spaces surrounding path slashes
 * - Safely URI-encodes special characters while preserving forward slashes
 * 
 * @param {string} branch - Raw branch input from user or payload
 * @param {string} defaultBranch - Fallback branch name
 * @returns {string} Sanitized and URL-safe branch path
 */
function sanitizeBranch(branch, defaultBranch = 'main') {
    if (!branch || typeof branch !== 'string' || !branch.trim()) return defaultBranch;
    const cleaned = branch.trim()
        .replace(/\s*\/\s*/g, '/')
        .replace(/\s+/g, '-');
    return cleaned
        .split('/')
        .map(segment => encodeURIComponent(segment))
        .join('/');
}

/**
 * Universal Repository Identifier Sanitizer
 * - Strips leading/trailing slashes and whitespace
 * - Ensures owner/repo format is safely URL-encoded
 * 
 * @param {string} repo - Raw repository input (e.g. "owner/repo")
 * @param {string} defaultRepo - Fallback repository
 * @returns {string} Sanitized and URL-safe repository path
 */
function sanitizeRepo(repo, defaultRepo = 'lokebuilds/core-auth-service') {
    if (!repo || typeof repo !== 'string' || !repo.trim()) return defaultRepo;
    const cleaned = repo.trim()
        .replace(/\s*\/\s*/g, '/')
        .replace(/\s+/g, '')
        .replace(/^\/+|\/+$/g, '');
    return cleaned
        .split('/')
        .map(segment => encodeURIComponent(segment))
        .join('/');
}

/**
 * Dynamic GitHub Compare URL Builder
 * Generates valid GitHub compare/pull-request URL with safely sanitized branches.
 * 
 * @param {string} repo - Repository (e.g. "lokebuilds/core-auth-service")
 * @param {string} base - Base branch (e.g. "main")
 * @param {string} head - Head/feature branch (e.g. "feat/login flow #42")
 * @returns {string} Full, safely encoded GitHub Compare URL
 */
function buildGitHubCompareUrl(repo, base, head) {
    const cleanRepo = sanitizeRepo(repo, 'lokebuilds/core-auth-service');
    const cleanBase = sanitizeBranch(base, 'main');
    const cleanHead = sanitizeBranch(head, 'feat/patch');
    return `https://github.com/${cleanRepo}/compare/${cleanBase}...${cleanHead}`;
}

// Export for module systems (Node / Bundlers) and browser window
if (typeof window !== 'undefined') {
    window.sanitizeBranch = sanitizeBranch;
    window.sanitizeRepo = sanitizeRepo;
    window.buildGitHubCompareUrl = buildGitHubCompareUrl;
}

if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        sanitizeBranch,
        sanitizeRepo,
        buildGitHubCompareUrl
    };
}
