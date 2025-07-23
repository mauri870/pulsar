const map = (line) => line
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ') // only letters and numbers
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map(word => [word, 1]);

const reduce = (key, values) => values.length; // count occurrences of each word