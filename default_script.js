const map = (line) => line
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ') // keep only letters and numbers, Unicode-aware
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map(word => [word, 1]);

const reduce = (key, values) => values.length; // count occurrences of each word