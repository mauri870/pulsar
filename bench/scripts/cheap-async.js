// Trivial async map: the cost is the engine, not the script.
const map = async line => line.split(' ').map(word => [word, 1]);
const reduce = async (key, values) => values.length;
