import fire
from pydantic import BaseModel
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
    cols: list[str] | None
    condition: Condition | JointCondition
    limit: int

# TODO: not a class
class Executor:
    cols: list[Col]

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
        if r is None: return Err(f"Could not match condition: {condition}")

        groups = r.groups()
        if len(groups) != 3: return Err(f"Regex failure on {condition}")
        lhs_literal, operator_literal, rhs_literal = groups

        lhs = self.parse_side(lhs_literal)
        if lhs.is_err(): return lhs # type: ignore
        lhs, lhs_type = lhs.unwrap() # type: ignore

        rhs = self.parse_side(rhs_literal)
        if rhs.is_err(): return rhs # type: ignore
        rhs, rhs_type = rhs.unwrap() # type: ignore

        operator = None
        match l := operator_literal.strip():
            case "=" | "!=":
                operator = EqualityOperator(l)
            case "<" | ">":
                # check that both are numbers
                if not (lhs_type == ColType.NUMBER and rhs_type == ColType.NUMBER):
                    return Err(f"Invalid comparison, lhs type: {lhs_type}, rhs type: {rhs_type}, operator: {l}")
                operator = InequalityOperator(l)
            case _:
                return Err(f"Invalid operator: {operator_literal}")
        return Ok(Condition(lhs=lhs_literal, operator=operator, rhs=rhs_literal))


    def parse_join_condition(self, condition: str) -> Result[Condition | JointCondition, str]:
        # check if condition is a joint condition (presence of AND or OR)
        # all of the non-whitespace characters should be consumed



    def parse_query(self, query: str) -> Result[Query, str]:
        pass


    def find_rows(self,
        query: Query, condition: Condition | JointCondition, limit: int
    ) -> Result[list[dict], str]:
        pass


    def select_cols(
        rows: list[dict], cols: list[str]
    ) -> Result[dict[Col, list[Literal]], str]:
        pass


    def execute_query(self, query: Query) -> Result[list[dict], str]:
        rows = find_rows(query, query.condition, limit)
        select_cols = select_cols(rows, query.cols, query.limit)


def main(json_path: str):
    # init: load the json file
    # loop
    # loop 1: take query
    # loop 2: execute query
    # loop 3: format query

    run = True
    while run:
        try:
            # take query
            query = input()

        except KeyboardInterrupt:
            print("Exiting...")
            run = False


fire.Fire(main)
