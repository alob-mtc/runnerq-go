package ui

import _ "embed"

// ConsoleHTML is the RunnerQ Console HTML content.
// Serve it via any HTTP framework to provide the queue monitoring UI.
//
//go:embed runnerq-console.html
var ConsoleHTML string
