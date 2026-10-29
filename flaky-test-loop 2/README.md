# Flaky test loop: run tests or the full CI many times, before and after a fix

A green CI run doesn't prove a flaky test is fixed. This GitHub Actions workflow runs your
tests, or the complete Flink CI, many times on your fork, on two commits:

- **before**: the code without your fix
- **after**: the code with your fix

It runs the tests the way Flink CI does: the same Docker image, JDK setup, non-root user,
Maven scripts and options.

## What's in this folder

| File | What it is |
|---|---|
| `flaky-test-loop.yml` | The workflow. All settings are at the top. |
| `ci-changes.patch` | Changes to `ci.yml` and `template.flink-ci.yml` that the workflow needs. |

The patch applies cleanly to master as of 2026-10-27 (`eead57e`). What it changes and why
is explained in the next section.

## What the patch changes in `ci.yml` and `template.flink-ci.yml`, and why

The workflow calls Flink's own CI template (`template.flink-ci.yml`), so the full CI mode
runs exactly the normal CI jobs. The template needs a few new options for that. All new
options have defaults that keep the normal CI as it is; the only visible difference is
that failure log downloads get `-copy1` in their name.

### `ci.yml`: don't start the normal CI on loop branches

```yaml
on:
  push:
    branches-ignore:          # added
      - 'flaky-loop/**'       # added
```
**Why:** without it, every push to your loop branch would also start the normal full CI
(about 2 hours, 13 more jobs) next to your loop, and use up your 20 jobs.

### `template.flink-ci.yml`

**1. New inputs**, next to the existing `environment` and `jdk_version`:
```yaml
      repository:   # default "" = the repository the workflow runs in
      ref:          # default "" = the commit that was pushed
      copies:       # default "[1]" = one copy
      packaging:    # default true = run the packaging job
```

**2. `repository` and `ref` are used in all 4 checkouts** (compile, packaging, test, E2E):
```yaml
        with:
          repository: ${{ inputs.repository || github.repository }}   # added
          ref: ${{ inputs.ref }}                                      # added
          persist-credentials: false
```
**Why:** normally the CI tests the commit you pushed. The loop has to test two other
commits (before and after), possibly from another fork, from one push.

**3. The packaging job can be skipped:**
```yaml
  packaging:
    needs: compile
    if: inputs.packaging      # added
```
**Why:** the packaging job only checks licenses, Javadocs and shaded jars; it runs no
tests and no other job waits for it. Skipping it saves one machine, which is what lets
1 before + 1 after CI fit into GitHub's 20 jobs at once.

**4. The test and E2E jobs run once per copy.** The test job's list of modules changes from
`include` entries to a plain list, and both jobs get a `copy` entry:
```yaml
      # before                               # after
      matrix:                                matrix:
        include:                               module: [core, python, table, connect, tests, misc]
          - module: core                       copy: ${{ fromJSON(inputs.copies) }}
            stringified-module-name: core
          - module: python
            ...
```
```yaml
      matrix:
        group: [1, 2, 3, 4]
        copy: ${{ fromJSON(inputs.copies) }}   # added (E2E job)
```
**Why:** GitHub creates one job for every combination of the lists, so 6 modules ×
`[1, 2]` = 12 test jobs, all using the same compile job. With the default `[1]` it's 6
jobs, as before. `include` can't be used for this: GitHub adds every `include` entry to
every combination, so a later entry would overwrite the module of an earlier one.
`stringified-module-name` always had the same value as `module`, so `module` replaces it.

**5. Job names and log download names include the copy:**
```yaml
    name: "Test (module: ${{ matrix.module }})${{ inputs.copies != '[1]' && format(' copy {0}', matrix.copy) || '' }}"
    name: "E2E (group ${{ matrix.group }})${{ inputs.copies != '[1]' && format(' copy {0}', matrix.copy) || '' }}"
    ...
          name: logs-test-...-${{ matrix.module }}-copy${{ matrix.copy }}-...
          name: logs-e2e-...-${{ matrix.group }}-copy${{ matrix.copy }}-...
```
**Why:** two copies of the same job would otherwise have the same name, and their log
downloads could clash. With one copy, job names stay as they are.

### Look at the changes yourself

Apply the patch on a throwaway branch and compare the files side by side:
```bash
git switch -c patch-preview master
git apply <this-folder>/ci-changes.patch
```
In VS Code, open **Source Control** (Ctrl+Shift+G) and click `ci.yml` or
`template.flink-ci.yml`: the old version is on the left, the new one on the right.
Afterwards, throw it away:
```bash
git restore .github/workflows && git switch - && git branch -D patch-preview
```
To see only a summary: `git apply --stat <this-folder>/ci-changes.patch`.

If the patch doesn't apply to a newer master, make the changes above by hand.

Test mode uses the same Docker image as the "Test" job of `template.flink-ci.yml`
(`apache/flink-ci-docker:..._noble_v2` on master as of 2026-10-27). If the template on your
branch uses another image, change the `image:` line of the `loop` job in
`flaky-test-loop.yml` to the same.

## Two modes

| `TEST` | What runs per commit |
|---|---|
| Your tests, e.g. `'MyTest#testA'` | Only those tests, `BEFORE_COUNT` / `AFTER_COUNT` times |
| `''` | The complete Flink CI, `BEFORE_COUNT` / `AFTER_COUNT` times |

## How it runs on GitHub Actions

Every job gets its own fresh machine (4 CPUs, 16 GB). The machines don't share anything.

**Test mode.** Example: `BEFORE_COUNT: 100`, `AFTER_COUNT: 100`, `SHARDS: 5`.
```
Settings (a few seconds)
 ├─ before: 5 machines, 20 runs each = 100 runs
 └─ after:  5 machines, 20 runs each = 100 runs      → 10 machines at once
```
Each machine builds `MODULE` and its dependencies once, then runs your tests again and
again, one run after another. If a count isn't a multiple of `SHARDS`, the first machines
get one run more. A count smaller than `SHARDS` uses one machine per run.

**Full CI mode.** Example: `BEFORE_COUNT: 1`, `AFTER_COUNT: 1`.
```
Settings (a few seconds)
 ├─ before: 1 compile → 1 CI = 10 jobs (6 test modules, 4 E2E groups)
 └─ after:  1 compile → 1 CI = 10 jobs                → 20 machines at once
```
With a count of 2, both CIs of a commit use the same compile job.

**Rounds.** `ROUNDS` runs everything again in the same workflow run, without another push.
Example: `ROUNDS: 3`, full CI, 1 before + 1 after:
```
Settings
 ├─ R1: before CI, R1: after CI     → 20 machines at once
 ├─ R2: before CI, R2: after CI     (queued; each starts when an R1 CI is done)
 └─ R3: before CI, R3: after CI     (queued)
```
`R1` is round 1, `R2` round 2, and so on.
GitHub runs at most one round's worth of jobs at once (`max-parallel`) and starts the next
ones as earlier ones finish. So round 2's before CI can start while round 1's after CI is
still running. Each full CI round compiles again. Every job keeps its own log.

With `STOP_ON_FIRST_FAILURE: true`, the first failure cancels everything still running or
queued, in all rounds. With `false`, everything runs to the end. To stop early, cancel the
workflow run.

**Count or rounds?** There are two ways to run the same commit more than once in full CI
mode:

| Setting | What happens | Job names |
|---|---|---|
| `BEFORE_COUNT: 2`, `ROUNDS: 1` | 2 complete CIs of the commit **at the same time**, in one round, sharing one Compile | `R1: before CI / Test (module: core) copy 1`, `... copy 2` |
| `BEFORE_COUNT: 1`, `ROUNDS: 2` | 1 complete CI of the commit, then another one **after it**, each with its own Compile | `R1: before CI / Test (module: core)`, `R2: before CI / Test (module: core)` |

Both give 2 complete CIs of the commit:
- A count of 2 is faster (about 1h35m for both), but it needs 20 machines for that one
  commit, so on a free account the other commit can't run at the same time.
- 2 rounds take about twice as long, but each round needs only 10 machines per commit, so
  1 before + 1 after fit into 20 machines.

To compare before and after on a free account, use `BEFORE_COUNT: 1`, `AFTER_COUNT: 1` and
`ROUNDS` for more runs. A count above 1 is for a plan with more machines, or for testing
only one commit. `copy N` only appears in job names when a count is 2 or more.

**The limit.** GitHub runs at most 20 jobs at once on a free account (Pro 40, Team 60,
Enterprise 500, see https://docs.github.com/en/actions/reference/limits). Set `MAX_JOBS`
to your plan's number. If the settings need more than `MAX_JOBS` jobs at once, the
Settings job stops the run with an error, before any other job starts.

The limit is shared with your other workflow runs, e.g. a normal CI run of your PR branch.
Jobs above the limit don't fail; they wait until a machine is free. (Seen on a free account:
a loop that asked for 24 machines got 19 while a CI job was running, and the other 5 waited.)

- Test mode needs one job per machine, so at most 2 × `SHARDS`.
- Full CI mode needs (`BEFORE_COUNT` + `AFTER_COUNT`) × 10. On a free account that's
  1 before + 1 after, or 2 of only one commit.
- `ROUNDS` doesn't change the number of jobs at once. GitHub allows at most 256 queued
  entries in total: one per commit and round in full CI mode, one per machine and round in
  test mode (e.g. 10 machines: at most 25 rounds). Settings checks this too.

## Steps

**1. Create a branch from your fix commit.** The name must start with `flaky-loop/`; the
rest is up to you.
```bash
git switch -c flaky-loop/FLINK-XXXXX <fix-commit>
```

**2. Add the workflow and apply the CI changes.**
```bash
cp <this-folder>/flaky-test-loop.yml .github/workflows/
git apply <this-folder>/ci-changes.patch
```

**3. Edit the settings at the top of `.github/workflows/flaky-test-loop.yml`.**

| Setting | What to put there |
|---|---|
| `BEFORE_COMMIT` | Commit ID or branch name of the code without the fix, usually `git rev-parse <fix-commit>^`. `''` skips it. |
| `BEFORE_REPO` | The fork to take it from, e.g. `'apache/flink'`. `''` means your fork. |
| `BEFORE_COUNT` | Test mode: number of test runs. Full CI: number of CI runs. `0` skips it. |
| `AFTER_COMMIT`, `AFTER_REPO`, `AFTER_COUNT` | The same for the code with the fix. |
| `TEST` | Your tests (see below), or `''` for the complete CI. Keep exactly one `TEST` line uncommented. |
| `MODULE` | Test mode: the Maven module that contains the tests, e.g. `'flink-runtime'`. |
| `SHARDS` | Test mode: the most machines per commit (default 5). |
| `ROUNDS` | How many times to run everything in the same workflow run (default `1`). |
| `STOP_ON_FIRST_FAILURE` | `true`: the first failure cancels everything still running or queued, in all rounds; in test mode, a machine also stops at its first failed run. `false`: everything runs to the end (default). |
| `JDK` | `11`, `17` or `21` (default `17`, the same as the normal CI). |
| `EXTRA_PROFILE` | Extra Maven options, e.g. `'-Penable-adaptive-scheduler'` to match that nightly build (default `''`). |
| `MAX_JOBS` | Jobs at once for your GitHub plan (default `20`, the free plan). |

`TEST` uses the Surefire `-Dtest` syntax:
- `Class#method`
- `Class#m1+m2`
- `ClassA,ClassB`
- `Blob*Test`

Example, test mode, 100 runs before and after:
```yaml
BEFORE_COMMIT: '<full id of the commit without the fix>'
BEFORE_COUNT: 100
AFTER_COMMIT: '<full id of your fix commit>'
AFTER_COUNT: 100
TEST: 'MyTest#testA+testB'
MODULE: 'flink-runtime'
```

Example, full CI, 1 before and 1 after:
```yaml
BEFORE_COMMIT: '<full id of the commit without the fix>'
BEFORE_COUNT: 1
AFTER_COMMIT: '<full id of your fix commit>'
AFTER_COUNT: 1
TEST: ''
```

The commits must exist in the repository you take them from: push your fix commit to
your fork first, or set `AFTER_REPO`.

**4. Commit and push to your fork.** Add only these 3 files, not your whole working tree.
```bash
git add .github/workflows/flaky-test-loop.yml .github/workflows/ci.yml .github/workflows/template.flink-ci.yml
git commit -m "[FLINK-XXXXX] Flaky test loop (do not merge)"
git push-external <your-fork-remote> flaky-loop/FLINK-XXXXX
```
The run starts by itself. Find it under **Actions → "Flaky test loop"** on your fork.

To run again with other settings, edit the settings, commit and push again.

Tip: for the first push use small counts (e.g. 1), so you see quickly that the setup works.

**5. Read the results.**
- The **Settings** job log shows what will run, e.g.
  `before: 100 run(s) of MyTest on 5 machine(s), ...`, `jobs at once: 10` and `rounds: 3`.
- Test mode: the run summary has one line per machine and round, e.g.
  `R1 before shard 3: 0 of 20 runs failed (20 planned)`. Add up the lines per commit.
  The jobs are named like `R1: before shard 3 · Build + Test (module: flink-runtime) × 20 runs`.
  Each job log shows every run with its test count, e.g.
  `run 7: passed | Tests run: 4, Failures: 0, ...`. Check this to be sure the tests really
  ran. If a run fails, its test reports and logs are attached to the run as a download.
- Full CI mode: the jobs are named `R1: before CI / Compile`, `R1: before CI / Test (module: core)` and so on,
  with `copy 1`, `copy 2` at the end when a count is 2 or more.
- Jobs cancelled by `STOP_ON_FIRST_FAILURE` are shown as cancelled.
  Any failing test anywhere in Flink turns that job red, so check which test failed.

**6. Clean up.** Delete the branch when you're done. Deleting the branch doesn't delete
the runs; GitHub keeps their logs for 90 days. **Never put these files in your PR.** The PR
should contain only the fix.

## Using another branch name

Any branch that starts with `flaky-loop/` works without changes. For a name that doesn't,
change it in 2 places:
- `flaky-test-loop.yml`, under `on: push: branches:`
- `ci.yml`, under `branches-ignore:`, so the push doesn't also start the normal CI

It can't be a setting under `env:`, because GitHub doesn't allow variables in the `on:`
block.

## Good to know

- **"before" may pass every run.** Flaky tests can fail rarely. In that case, show why the
  test was flaky in another way, for example a small program that forces the race. Then
  present both: "the old code fails when the race is forced, and the new code passes".
- **Timeouts:**
  - Each test run may take up to `RUN_TIMEOUT_MINUTES` (240, the same as CI). A run that
    hangs is stopped, counted as `TIMEOUT`, and the next run starts.
  - Each job may take up to 360 minutes, GitHub's maximum. In test mode, the runs of one
    machine have to fit into that, so use more `SHARDS`, smaller counts or more `ROUNDS`
    for slow tests.
- **Measured times:**
  - One complete CI run on a fork took 2h13m (normal CI, with the packaging job).
  - Full CI mode, 1 before + 1 after in parallel, one round: 1h34m.
  - Test mode, 2 small tests in `flink-runtime`: building took 7–10 minutes, then each run
    took about 23–25 seconds.
- **Cost:** a public fork runs on GitHub's runners for free. Nothing runs on apache/flink's
  CI.
- **Differences from CI in test mode:**
  - only `MODULE` and its dependencies are built;
  - the CI watchdog isn't used.
