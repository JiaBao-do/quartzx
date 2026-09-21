# Changelog

All notable changes are documented here. Format: [Keep a Changelog](https://keepachangelog.com/), versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Added
- Quartz-compatible cron parser (`ParseCron`) with seconds, `? L W #`, `L-n`, `LW`, year field, names and descriptors.
- Injectable `Clock` with a deterministic `FakeClock`.
