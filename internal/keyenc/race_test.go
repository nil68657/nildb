//go:build race

package keyenc

// raceEnabled relaxes the property test's time budget, which assumes a
// build without the race detector.
const raceEnabled = true
