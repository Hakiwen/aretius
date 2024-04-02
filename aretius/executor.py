import json
from pydantic import BaseModel, ConfigDict
from result import Result, Ok, Err
import re
import enum
import ast


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
                if k not in cols:
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
            return Ok((literal_match.group(), ColType.STRING))

        # is it a number?
        # NOTE: may be error prone
        number_match = re.match(r"\d+", side)
        if number_match:
            return Ok((ast.literal_eval(number_match.group()), ColType.NUMBER))

        # is it a column name?
        col_match = re.match(r"\w+", side)
        for c in self.cols:
            if c.name == col_match:
                return Ok((c, c.type))
        return Err(f"Syntax Error: could not parse {side}")

    def parse_condition(self, condition: str) -> Result[Condition, str]:
        r = re.match(r"(\w+)\s*(=|!=)\s*(\w+)", condition)
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
        return Ok(Condition(lhs=lhs_literal, operator=operator, rhs=rhs_literal))

    # TODO: parens and nesting
    def parse_joint_condition(
        self, condition: str
    ) -> Result[Condition | JointCondition, str]:
        # check if condition is a joint condition (presence of AND or OR)
        initial = re.match(r"(\w+)\s*(AND|OR)\s*(\w+)", condition)
        if initial is None:
            return self.parse_condition(condition)
        lhs = self.parse_condition(initial.group(1))
        if lhs.is_err():
            return lhs
        rhs = self.parse_condition(initial.group(3))
        if rhs.is_err():
            return rhs

        try:
            operator = JoinOperator(initial.group(2))
        except Exception:
            return Err(f"Invalid operator: {initial.group(2)}")

        return Ok(
            JointCondition(
                lhs=lhs.unwrap(),
                operator=operator,
                rhs=rhs.unwrap(),
            )
        )

        # all of the non-whitespace characters should be consumed

    def parse_cols(self, cols_literal: str) -> Result[list[Col], str]:
        if cols_literal == "*":
            cols = self.cols
        else:
            cols = []
            for c in cols_literal.split(","):
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
            r"SELECT\s+(.+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?(?:\s+LIMIT\s+(\d+))?",
            query,
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
        return True

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

    def execute_query(self, query: Query) -> Result[dict[Col, list[Literal]], str]:
        rows = self.find_rows(query.condition, query.limit)
        if rows.is_err():
            return rows  # type: ignore
        rows = rows.unwrap()

        res = {}
        for c in query.cols:
            res[c] = []
            for r in rows:
                res[c].append(self.rows[r][c.name])
        return Ok(res)

    def __call__(self, query: str) -> Result[dict[Col, list[Literal]], str]:
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
    r = sqle("SELECT * FROM table LIMIT 5")
    match r:
        case Ok(q):
            print(q)
        case Err(e):
            print(e)
