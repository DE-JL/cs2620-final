import random
import string
import json
import os

from palindromic_substring import longest_palindromic_substring


def random_alphanumeric_string(n):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))


def generate_lower() -> list[tuple[str, str]]:
    test_cases: list[tuple[str, str]] = []
    for i in range(10):
        s = random_alphanumeric_string(100)
        test_cases.append((s, s.lower()))

    return test_cases


def generate_sorted() -> list[tuple[str, str]]:
    test_cases: list[tuple[str, str]] = []
    for i in range(10):
        s = random_alphanumeric_string(100)
        s_sorted = ''.join(sorted(s))
        test_cases.append((s, s_sorted))

    return test_cases


def generate_pal() -> list[tuple[str, str]]:
    def random_alphanumeric(n):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

    # Generate test cases
    test_cases: list[tuple[str, str]] = []
    for i in range(10):
        s = random_alphanumeric(100)
        lps = longest_palindromic_substring(s)
        test_cases.append((s, str(lps)))

    test_cases.append(("a" * 100, "100"))

    return test_cases


def write_test_cases_to_json(test_cases: list[tuple[str, str]], filename="test_bank/test_cases.json"):
    json_test_cases = [list(case) for case in test_cases]

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Write to JSON file
    with open(filename, 'w') as f:
        json.dump(json_test_cases, f, indent=4)

    print(f"Test cases written to {filename}")


def main():
    # Generate test cases
    test_cases = generate_sorted()

    # Write to JSON
    write_test_cases_to_json(test_cases)


if __name__ == "__main__":
    main()
