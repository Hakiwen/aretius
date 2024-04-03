import fire
from aretius.executor import Executor, Col, Literal
from result import Ok, Err


if __name__ == "__main__":

    def main(json_path: str):
        sql_executor_r = Executor.from_json_file(json_path)
        if sql_executor_r.is_err():
            print(sql_executor_r.unwrap_err())
            return
        sql_executor = sql_executor_r.unwrap()

        run = True
        while run:
            try:
                # take query
                print(">:", end=" ")
                query = input()

                match r := sql_executor(query):
                    case Ok(q):
                        print(q)
                    case Err(e):
                        print("Encountered an error:")
                        print(e)
            except KeyboardInterrupt:
                print("Exiting...")
                run = False

    fire.Fire(main)
