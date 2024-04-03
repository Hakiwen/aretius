import json
import re

import pandas as pd
import duckdb

from aretius.executor import Executor

with open("tests/test_commands.json", "r") as f:
    commands = json.load(f)
m = Executor.from_json_file("tests/flat_state.json")
if not m.is_err():
    executor = m.unwrap()
df = pd.read_json("tests/flat_state.json")

for command in commands:
    try:
        e = executor(command).unwrap()
        val = duckdb.query(re.sub("TABLE", "df", command)).to_df()
        if e.empty:
            assert val.empty
            continue
        merged_df = pd.merge(e, val, indicator=True, how="outer")

        # Check if all rows have '_merge' value of 'both'
        same_rows = (merged_df["_merge"] == "both").all()
        assert same_rows
    except:
        continue
