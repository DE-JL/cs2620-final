import asyncio
import subprocess
import textwrap
import time

from config import config
from protos.autograder_pb2 import SubmissionRequest, SubmissionResponse, Status


class Job:
    def __init__(self, submission: SubmissionRequest):
        # Store the submission
        self.submission = submission

        # Store state for asynchronous execution
        self.loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        self.future = self.loop.create_future()

        # Keep track of the times
        self.enqueue_time = time.time()
        self.dequeue_time = None
        self.execution_start_time = None
        self.execution_end_time = None

    def run(self):
        # Record the time of dequeue
        self.dequeue_time = time.time()

        # Record the time of execution
        self.execution_start_time = time.time()
        result = run_submission(self.submission)
        self.execution_end_time = time.time()

        # Set the result
        self.loop.call_soon_threadsafe(self.future.set_result, result)

    def get_queued_time(self):
        return self.dequeue_time - self.enqueue_time

    def get_execution_time(self):
        return self.execution_end_time - self.execution_start_time


def run_submission(submission: SubmissionRequest) -> SubmissionResponse:
    task: dict = config["tasks"][str(submission.task_id)]
    test_cases: list[tuple[str, str]] = task["test_cases"]

    for idx, (input_data, expected_output) in enumerate(test_cases):
        result = run_test_case(submission.source_code, input_data, expected_output, idx)
        if result is not None:
            return result

    return SubmissionResponse(status=Status.OK,
                            output="All test cases passed.")


def run_test_case(code: str, input_data: str, exp: str, idx: int) -> SubmissionResponse | None:
    try:
        result = subprocess.run(["python3", "-c", textwrap.dedent(code)],
                                input=input_data.encode(),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                timeout=2)
        output = result.stdout.decode().strip()

        if output != exp.strip():
            return SubmissionResponse(status=Status.ERROR,
                                    output=f"Incorrect output for test case {idx + 1}! "
                                           f"Expected: {exp}, Got: {output}")
        return None

    except subprocess.TimeoutExpired:
        return SubmissionResponse(status=Status.ERROR,
                                output=f"Timed out on test case {idx + 1}")
    except Exception as e:
        return SubmissionResponse(status=Status.ERROR,
                                output=f"Execution failed: {e}")
