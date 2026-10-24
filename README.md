# Tourbillon

Tourbillon keeps a log on disk that survives a crash, and copies it over several
machines so that they agree on what is in it, even when one of them fails. A
program hands an entry to Tourbillon; once Tourbillon calls it accepted, the
entry is safe, and every machine gets it at the same place in the same order.
Tourbillon does not know what the entries mean: the program that uses it turns
them into its own state.

Tourbillon is a library, not a product: the program that uses it builds it in,
and the application that hosts that program decides where it runs.

This is a preview: the package is `0.1.0-preview.1` and the API is not stable yet.

## License

MIT, Copyright (c) 2026 Wouter Naessens. See [LICENSE](LICENSE).
