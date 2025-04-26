import json
import os
import random
import string

from solution_pal import longest_palindromic_substring


def get_random_str(n):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))


def generate_lower():
    test_cases: list[tuple[str, str]] = []

    for i in range(100):
        s = get_random_str(1000)
        test_cases.append((s, s.lower()))

    publish_test_cases(test_cases, "test_bank/test_cases_lower.json")


def generate_sorted():
    test_cases: list[tuple[str, str]] = []

    for i in range(100):
        s = get_random_str(1000)
        s_sorted = ''.join(sorted(s))
        test_cases.append((s, s_sorted))

    publish_test_cases(test_cases, "test_bank/test_cases_sorted.json")


def generate_pal():
    test_cases: list[tuple[str, str]] = []

    for i in range(100):
        s = get_random_str(1000)
        lps = longest_palindromic_substring(s)
        test_cases.append((s, str(lps)))

    # Some edge cases
    test_cases.append(("a" * 100, "100"))
    test_cases.append(("a" * 100 + "b" * 101, "101"))
    test_cases.append(("racecar", "7"))

    publish_test_cases(test_cases, "test_bank/test_cases_pal.json")


def publish_test_cases(test_cases: list[tuple[str, str]],
                       filename: str = "test_bank/test_cases.json"):
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Write to JSON file
    with open(filename, 'w') as f:
        json.dump(test_cases, f, indent=4)

    print(f"Test cases written to {filename}")


def main():
    generate_lower()
    generate_sorted()
    generate_pal()


if __name__ == "__main__":
    main()
