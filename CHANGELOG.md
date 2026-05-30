# Changelog

## 1.0.0

- Added job continuations with `BackgroundJobClient.continue_with()`.
- Added distributed lock support for storage backends and a
  `DisableConcurrentExecution` filter.
- Added state-handler extension hooks around state transitions.
- Moved backend, integration, docs, and development tooling dependencies into
  optional extras or the development dependency group.
- Verified the package with linting, formatting, type checking, tests, docs, and
  build checks.
