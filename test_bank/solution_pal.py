def expand(s: str, l: int, r: int) -> int:
    radius = 0

    while 0 <= l and r < len(s) and s[l] == s[r]:
        radius += 1
        l -= 1
        r += 1

    return radius


def longest_palindromic_substring(s: str) -> int:
    longest = 0

    for i in range(len(s)):
        odd_length = 2 * expand(s, i, i) - 1
        even_length = 2 * expand(s, i, i + 1)
        longest = max(longest, odd_length, even_length)

    return longest


def main():
    s = input()
    print(longest_palindromic_substring(s))


if __name__ == "__main__":
    main()
