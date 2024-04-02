import json


def test_load():
    path = "tests/state.json"
    with open(path, "r") as f:
        data = json.load(f)
