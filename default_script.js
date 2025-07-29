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
// Used to make assertions about the behavior of the map, reduce, and sort functions.
// This function is only executed when the `--test` flag is passed.
const test = async () => {
    const input = "The quick brown fox jumps over the lazy dog";
    const result = await map(input);
    const expected = [
        ["the", 1],
        ["quick", 1],
        ["brown", 1],
        ["fox", 1],
        ["jumps", 1],
        ["over", 1],
        ["the", 1],
        ["lazy", 1],
        ["dog", 1]
    ];
    const str = JSON.stringify;
    if (str(result) !== str(expected)) {
        throw new Error(`Map test failed: expected ${str(expected)}, got ${str(result)}`);
    }

    const key = "the";
    const values = [1, 1, 1];
    const reduced = await reduce(key, values);
    if (reduced !== 3) {
        throw new Error(`Reduce test failed: expected 3, got ${reduced}`);
    }

    const unsorted = [["zebra", 1], ["apple", 1], ["monkey", 1]];
    const sorted = await sort([...unsorted]);
    const sortedExpected = [["apple", 1], ["monkey", 1], ["zebra", 1]];
    if (str(sorted) !== str(sortedExpected)) {
        throw new Error(`Sort test failed:\nExpected: ${str(sortedExpected)}\nGot: ${str(sorted)}`);
    }
};
