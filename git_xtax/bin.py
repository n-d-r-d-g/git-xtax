import sys


def main():
    if sys.version_info[:2] < (3, 8):
        version_str = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        sys.stderr.write(
            f"Python {version_str} is not supported.\n"
            "Please switch to Python 3.8 or higher.\n")
        sys.exit(1)

    from . import cli
    cli.main()
