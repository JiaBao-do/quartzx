# quartzx status

| stage | attempts | updated | evidence |
|-------|----------|---------|----------|
| validated (PARTIAL gap) | 0 | 2026-09-21 | Step 0 done, see below. Building. |

## Step 0 evidence (2026-09-21)

Read READMEs and shallow-cloned sources of gocron and go-quartz (grep of non-test .go files).

| lib | stars / last push | deps | persistence | misfire | calendars | cron |
|-----|-------------------|------|-------------|---------|-----------|------|
| reugn/go-quartz v0.15.2 (MIT) | 2012 / 2026-01-20 | 0 | `JobQueue` interface, only an example file-system queue in examples/ | `WithMisfiredChan` notification channel and `WithOutdatedThreshold`; no policies | none (0 matches) | full Quartz: sec, L, W, #, ?, year |
| go-co-op/gocron v2.22.0 (MIT) | 7166 / 2026-09-01 | 3 non-test (uuid, clockwork, robfig/cron) + testify, goleak | none (0 matches; elector/locker for HA only) | none (0 matches) | none | via robfig/cron |
| robfig/cron v3 (MIT) | 14188 / 2024-07-08 (no release since) | 0 | none | none | none | 5 field, optional seconds, no L/W/#/? year |

Others seen and out of scope: dkron, dagu (services), river, asynq (queues), flier/quartz (2015).
GitHub search API hit the rate limit after the first round of queries; repo view / contents API and clones were used instead.

Differentiator (what neither covers): named misfire policies (fire-now, skip, reschedule, fire-all) with threshold;
first-class `Store` interface with in-memory and atomic-file JSON stores plus restart recovery of missed fires;
calendars/holiday exclusions; calendar-interval triggers; non-overlap per job by default; injectable fake clock shipped;
documented DST semantics with reference table; Go 1.24 floor, zero deps.

Verdict: PARTIAL, continue.

## Board

agentboard RUNNING.md did not exist at check time (2026-09-21); nothing synced.

## Log
