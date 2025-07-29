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

// Test function:
// This function is called when the test command is run.
const test = async () => {
    const input = "The quick brown fox jumps over the lazy dog";
    const result = await map(input);
    const expectedWords = [
        "the", "quick", "brown", "fox",
        "jumps", "over", "the", "lazy", "dog"
    ];
    if (result.length !== expectedWords.length) {
        throw new Error(`Map test failed: expected ${expectedWords.length} pairs, got ${result.length}`);
    }
    for (let i = 0; i < result.length; i++) {
        const [word, count] = result[i];
        if (word !== expectedWords[i] || count !== 1) {
            throw new Error(`Map test failed at index ${i}: expected [${expectedWords[i]}, 1], got [${word}, ${count}]`);
        }
    }

    // === Test reduce ===
    const key = "the";
    const values = [1, 1, 1];
    const reduced = await reduce(key, values);
    if (reduced !== 3) {
        throw new Error(`Reduce test failed: expected 3, got ${reduced}`);
    }

    // === Test sort ===
    const unsorted = [["zebra", 1], ["apple", 1], ["monkey", 1]];
    const sorted = await sort([...unsorted]); // clone to avoid mutating original
    const sortedExpected = [["apple", 1], ["monkey", 1], ["zebra", 1]];
    const str = JSON.stringify;
    if (str(sorted) !== str(sortedExpected)) {
        throw new Error(`Sort test failed:\nExpected: ${str(sortedExpected)}\nGot: ${str(sorted)}`);
    }
};
