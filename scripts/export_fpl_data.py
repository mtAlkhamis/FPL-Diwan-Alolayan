name: Update FPL Recap Data

on:
  workflow_dispatch:  # run manually from the Actions tab whenever you want fresh data

permissions:
  contents: write  # needed so the commit step below can push data/fpl-data.json back to the repo

concurrency:
  # If this gets triggered twice close together (e.g. clicking "Run workflow"
  # twice, or a second manual run before the first finishes), queue the
  # second one instead of letting both race to push at the same time - that
  # race is what caused the "non-fast-forward" push rejection.
  group: update-fpl-data
  cancel-in-progress: false

jobs:
  update-data:
    runs-on: ubuntu-latest
    steps:
      - name: Check out repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install pandas requests fire

      - name: Run export script
        # League ID 14514 - Best in Q8's league. Script lives in the
        # scripts/ folder of this repo. season_history_earliest_season
        # caps the past-seasons archive at this league's own first season
        # (2013/14) so members' personal FPL history from before this
        # league existed doesn't show up as if it were part of it.
        run: python3 scripts/export_fpl_data.py --league_id=14514 --end_gw=0 --season_history_earliest_season=2013/14

      - name: Commit updated data
        uses: stefanzweifel/git-auto-commit-action@v5
        with:
          commit_message: "Update FPL recap data"
          file_pattern: data/fpl-data.json
