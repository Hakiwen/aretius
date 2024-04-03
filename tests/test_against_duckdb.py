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
    r = executor(command).unwrap()
    r_df = duckdb.query(re.sub("TABLE", "df", command)).to_df()
    if r.empty:
        assert r_df.empty
    else:
        assert r.equals(r_df)
    print(r)
    print(r_df)
    print(command)
    # query = command["query"]
    # result = executor(query)
    # if result.is_err():
    #     print(result.unwrap_err())
    # else:
    #     print(result.unwrap())
# df = pd.read_json("tests/flat_state.json")
