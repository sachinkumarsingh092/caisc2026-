# EVOLVE-BLOCK-START
import math

def solve():
    # Try to compute a meaningful value
    # Explore: return a computed result based on common optimization targets
    n = 100
    total = 0
    for i in range(1, n + 1):
        total += i * i
    # Return sum of squares formula result, which equals n(n+1)(2n+1)/6
    return total
# EVOLVE-BLOCK-END

if __name__ == "__main__":
    print(solve())
