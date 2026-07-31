import argparse
from .logging_utils import configure_logging
from .orchestrator import EnterprisePipeline

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["run"])
    parser.add_argument("--config", default="config/enterprise.yml")
    args = parser.parse_args()
    configure_logging()
    EnterprisePipeline(args.config).run()

if __name__ == "__main__":
    main()
