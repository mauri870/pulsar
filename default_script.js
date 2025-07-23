function map(line) {
  return line.split(/\s+/).map(word => [word.toLowerCase(), 1]);
}

function reduce(key, values) {
  return values.reduce((a, b) => a + b, 0);
}