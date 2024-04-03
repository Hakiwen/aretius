import json
from pydantic import BaseModel, ConfigDict
from result import Result, Ok, Err
import re
import enum
import ast
import pandas as pd


class EqualityOperator(enum.Enum):
    EQUALS = "="
    NOT_EQUALS = "!="


class InequalityOperator(enum.Enum):
    LESS_THAN = "<"
    GREATER_THAN = ">"


class JoinOperator(enum.Enum):
    AND = "AND"
    OR = "OR"


ClauseOperator = EqualityOperator | InequalityOperator
Operator = ClauseOperator | JoinOperator
Literal = str | int | float


class ColType(enum.Enum):
    STRING = "string"
    NUMBER = "number"


class Col(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str
    type: ColType


# TODO: do condition and joint condition need to be sep types?
# TODO: comparison between columns
class Condition(BaseModel):
    lhs: Col | Literal
    operator: ClauseOperator
    rhs: Col | Literal


class JointCondition(BaseModel):
    lhs: "Condition | JointCondition"
    operator: JoinOperator
    rhs: "Condition | JointCondition"


class Query(BaseModel):
    cols: list[Col]
    condition: Condition | JointCondition | None
    limit: int | None


# TODO: not a class
class Executor(BaseModel):
    cols: list[Col]
    rows: list[dict]

    @classmethod
    def from_json_file(cls, path: str) -> Result["Executor", str]:
        with open(path, "r") as f:
            data = json.load(f)
        return Executor.init(data)

    # TODO: validate loaded data
    @classmethod
    def init(cls, data: list[dict]) -> Result["Executor", str]:
        cols = []
        for row in data:
            for k, v in row.items():
                if k not in [s.name for s in cols]:
                    if isinstance(v, str):
                        cols.append(Col(name=k, type=ColType.STRING))
                    elif isinstance(v, int) or isinstance(v, float):
                        cols.append(Col(name=k, type=ColType.NUMBER))
                    else:
                        return Err(f"Invalid type: {type(v)}")
        return Ok(Executor(cols=cols, rows=data))

    def parse_side(self, side: str) -> Result[tuple[Col | Literal, ColType], str]:
        # is it a string literal?
        literal_match = re.match(r"\'.*\'", side)
        if literal_match:
            return Ok((literal_match.group().strip("'"), ColType.STRING))

        # is it a number?
        # NOTE: may be error prone
        number_match = re.match(r"\d+", side)
        if number_match:
            return Ok((ast.literal_eval(number_match.group()), ColType.NUMBER))

        # is it a column name?
        col_match = re.match(r"\w+", side)
        if col_match is None:
            return Err(f"Syntax Error: could not match {side}")
        for c in self.cols:
            if c.name == col_match.group():
                return Ok((c, c.type))
        return Err(f"Syntax Error: could not parse {side}")

    def parse_condition(self, condition: str) -> Result[Condition, str]:
        r = re.match(r"('[^']*'|[\w]+)\s*(=|!=|<|>)\s*('[^']*'|[\w]+)", condition)
        if r is None:
            return Err(f"Could not match condition: {condition}")

        groups = r.groups()
        if len(groups) != 3:
            return Err(f"Regex failure on {condition}")
        lhs_literal, operator_literal, rhs_literal = groups

        lhs = self.parse_side(lhs_literal)
        if lhs.is_err():
            return lhs  # type: ignore
        lhs, lhs_type = lhs.unwrap()  # type: ignore

        rhs = self.parse_side(rhs_literal)
        if rhs.is_err():
            return rhs  # type: ignore
        rhs, rhs_type = rhs.unwrap()  # type: ignore

        operator = None
        match l := operator_literal.strip():
            case "=" | "!=":
                operator = EqualityOperator(l)
            case "<" | ">":
                # check that both are numbers
                if not (lhs_type == ColType.NUMBER and rhs_type == ColType.NUMBER):
                    return Err(
                        f"Invalid comparison, lhs type: {lhs_type}, rhs type: {rhs_type}, operator: {l}"
                    )
                operator = InequalityOperator(l)
            case _:
                return Err(f"Invalid operator: {operator_literal}")
        return Ok(Condition(lhs=lhs, operator=operator, rhs=rhs))

    def build_joint_condition_tree(self, result_list):
        if isinstance(result_list, Condition):
            return result_list
        if len(result_list) == 1:
            item = result_list[0]
            if isinstance(item, list):
                return self.build_joint_condition_tree(item)
            else:
                # Assuming item is a Condition object
                return item

        lhs = self.build_joint_condition_tree(result_list[0])
        operator = JoinOperator(
            result_list[1]
        )  # Assuming result_list[1] is a valid JoinOperator
        rhs = self.build_joint_condition_tree(result_list[2])

        return JointCondition(lhs=lhs, operator=operator, rhs=rhs)

    def parse_joint_condition(
        self, condition: str
    ) -> Result[Condition | JointCondition, str]:
        stack = []
        result = []
        tokens = re.findall(
            r"(\(|\)|\bAND\b|\bOR\b|[^()\s]+\s*(?:=|<|>|!=)\s*(?:'[^']*'|[^()\s]+))",
            condition,
            flags=re.IGNORECASE,
        )

        for token in tokens:
            if token == "(":
                stack.append(result)
                result = []
            elif token == ")":
                group = result
                result = stack.pop()
                result.append(group)
            elif token.upper() in ["AND", "OR"]:
                result.append(token.upper())
            else:
                atomic_condition_literal = token.strip()
                atomic_condition = self.parse_condition(atomic_condition_literal)
                if atomic_condition.is_err():
                    return atomic_condition
                result.append(atomic_condition.unwrap())
        if len(result) == 1:
            return Ok(result[0])
        return Ok(self.build_joint_condition_tree(result))

    def parse_cols(self, cols_literal: str) -> Result[list[Col], str]:
        if cols_literal.strip() == "*":
            cols = self.cols
        else:
            cols = []
            for c in cols_literal.split(","):
                c = c.strip()
                for s in self.cols:
                    if c == s.name:
                        cols.append(s)
                        break
                else:
                    return Err(f"Invalid column name: {c}")
        return Ok(cols)

    # TODO: should column names be case-insensitive?
    def parse_query(self, query: str) -> Result[Query, str]:
        groups = re.match(
            r"SELECT\s+(.+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+LIMIT\s+(\d+))?(?:;)?$",
            query,
            re.IGNORECASE,
        )
        if groups is None:
            return Err(f"Could not parse top-level query: {query}")

        cols_literal = groups.group(1)
        cols = self.parse_cols(cols_literal)
        if cols.is_err():
            return cols  # type: ignore
        cols = cols.unwrap()

        table_literal = groups.group(2)
        if table_literal.lower() != "table":
            return Err(
                f"Invalid table name: {table_literal}. Can only be 'table' or 'TABLE'"
            )

        condition = None
        # if group 4 is not none, all possible are present
        # TODO: check limit is valid number
        if groups.group(3) is not None:
            condition = self.parse_joint_condition(groups.group(3))
            if condition.is_err():
                return condition  # type: ignore
            condition = condition.unwrap()

        limit = None
        if groups.group(4) is not None:
            limit = int(groups.group(4))

        return Ok(
            Query(
                cols=cols,
                condition=condition,
                limit=limit,
            )
        )

    def evaluate_row_condition(
        self, row: dict, condition: Condition | JointCondition | None
    ) -> bool:
        if condition is None:
            return True
        if isinstance(condition, JointCondition):
            match condition.operator:
                case JoinOperator.AND:
                    return self.evaluate_row_condition(
                        row, condition.lhs
                    ) and self.evaluate_row_condition(row, condition.rhs)
                case JoinOperator.OR:
                    return self.evaluate_row_condition(
                        row, condition.lhs
                    ) or self.evaluate_row_condition(row, condition.rhs)
                case _:
                    assert False, "unreachable"

        if isinstance(condition.lhs, Col):
            lhs = row[condition.lhs.name]
        else:
            lhs = condition.lhs
        if isinstance(condition.rhs, Col):
            rhs = row[condition.rhs.name]
        else:
            rhs = condition.rhs
        match condition.operator:
            case EqualityOperator.EQUALS:
                return lhs == rhs
            case EqualityOperator.NOT_EQUALS:
                return lhs != rhs
            case InequalityOperator.LESS_THAN:
                assert (isinstance(lhs, int) or isinstance(lhs, float)) and (
                    isinstance(rhs, int) or isinstance(rhs, float)
                )
                return lhs < rhs
            case InequalityOperator.GREATER_THAN:
                assert (isinstance(lhs, int) or isinstance(lhs, float)) and (
                    isinstance(rhs, int) or isinstance(rhs, float)
                )
                return lhs > rhs
            case _:
                assert False, "unreachable"

    def find_rows(
        self, condition: Condition | JointCondition | None, limit: int | None
    ) -> Result[list[int], str]:
        if condition is None and limit is None:
            return Ok(list(range(len(self.rows))))
        row_indices: list[int] = []
        for i, row in enumerate(self.rows):
            if self.evaluate_row_condition(row, condition):
                row_indices.append(i)

            if limit and len(row_indices) >= limit:
                break
        return Ok(row_indices)

    def execute_query(self, query: Query) -> Result[pd.DataFrame, str]:
        rows = self.find_rows(query.condition, query.limit)
        if rows.is_err():
            return rows  # type: ignore
        rows = rows.unwrap()

        data = {}
        for c in query.cols:
            data[c.name] = []
            for r in rows:
                data[c.name].append(self.rows[r][c.name])
        df = pd.DataFrame(data)
        return Ok(df)

    def __call__(self, query: str) -> Result[pd.DataFrame, str]:
        q = self.parse_query(query)
        if q.is_err():
            return q  # type: ignore
        q = q.unwrap()
        return self.execute_query(q)


if __name__ == "__main__":
    import os

    script_dir = os.path.dirname(os.path.realpath(__file__))
    sqle = Executor.from_json_file(
        os.path.join(script_dir, "..", "tests", "flat_state.json")
    )
    sqle = sqle.unwrap()
    r = sqle("SELECT * FROM table WHERE POP_2021 > 10000000")
    match r:
        case Ok(q):
            print(q.head(5))
        case Err(e):
            print(e)
