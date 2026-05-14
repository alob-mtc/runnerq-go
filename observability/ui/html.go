package ui

import (
	"embed"
	"io/fs"
)

// ConsoleHTML is the RunnerQ Console HTML content.
// Serve it via any HTTP framework to provide the queue monitoring UI.
//
//go:embed runnerq-console.html
var ConsoleHTML string

// vendorFS holds the third-party JS dependencies the console needs at
// runtime (React, ReactDOM, Babel-standalone). Vendoring them lets the
// console run behind strict CSPs (`script-src 'self'`) and on networks
// that can't reach unpkg.com — at the cost of ~3 MB of binary size,
// almost all of which is Babel.
//
//go:embed vendor
var vendorFS embed.FS

// VendorAssets returns the embedded vendor directory rooted at the directory
// itself, so paths look like "react.production.min.js" rather than
// "vendor/react.production.min.js". Used by the console's /vendor/ route.
func VendorAssets() fs.FS {
	sub, err := fs.Sub(vendorFS, "vendor")
	if err != nil {
		// Should be impossible — the embed directive guarantees the path exists.
		panic("ui: vendor sub-fs unavailable: " + err.Error())
	}
	return sub
}
