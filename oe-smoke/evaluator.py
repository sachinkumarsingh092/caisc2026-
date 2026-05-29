"""Smoke evaluator: program should print 42. Score = 1/(1+|val-42|)."""
import subprocess


def evaluate(program_path: str) -> dict:
    try:
        out = subprocess.run(
            ["python3", program_path],
            capture_output=True,
            text=True,
            timeout=5,
        )
        val = int(out.stdout.strip())
        diff = abs(val - 42)
        return {"score": 1.0 / (1.0 + diff), "value": val}
    except Exception as e:
        return {"score": 0.0, "error": str(e)}
