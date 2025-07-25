// Map function:
// Receives a single line of input.
// Should return an array of [key, value] pairs to emit from this line.
const map = async line => line
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ') // only letters and numbers
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map(word => [word, 1]);

// Reduce function:
// Receives a key and all values associated with that key.
// Should return a aggregated value for the key.
const reduce = async (key, values) => values.length; // count occurrences of each word

// Optional Sort function:
// Enabled only with --sort. Receives the full list of [key, value] pairs after reduction.
// Should return a sorted list. If sorting is enabled it disables streaming output
// since all results must be collected into memory before sorting.
const sort = async (results) =>
    results.sort((a, b) => a[0].localeCompare(b[0])) // Sort alphabetically
