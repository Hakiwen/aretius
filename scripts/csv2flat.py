import json
import fire


def main(input_path: str, output_path: str):
    with open(input_path, "r") as f:
        data = json.load(f)

    col_names = data[0]
    output = []
    for row in data[1:]:
        for i, value in enumerate(row):
            if value.isdigit():
                row[i] = int(value)
        output.append(dict(zip(col_names, row)))
    with open(output_path, "w") as f:
        json.dump(output, f, indent=4)


fire.Fire(main)
