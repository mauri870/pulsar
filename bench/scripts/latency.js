const map = async line => {
  // Simulate processing delay once per line
  await doWork();

  return line
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ') // keep only letters and numbers
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map(word => [word, 1]);
};

const doWork = async () => {
  const delay = 0.05 + Math.random() * 0.4;
  const start = performance.now();
  while ((performance.now() - start) < delay) {
    await new Promise(resolve => setImmediate(resolve));
  }
}

const reduce = async (key, values) => values.length; // count occurrences of each word

const sort = async (results) =>
    results.sort((a, b) => a[0].localeCompare(b[0])) // Sort alphabetically
