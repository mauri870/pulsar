// Trivial plain-function map: the same work as cheap-async without promises.
const map = line => {
  const pairs = [];
  for (const word of line.split(' ')) pairs.push([word, 1]);
  return pairs;
};
const reduce = (key, values) => values.length;
