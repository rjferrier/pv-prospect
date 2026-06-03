"""Entry point: python -m pv_prospect.model_trainer <subcommand> ..."""

from pv_prospect.common import configure_logging
from pv_prospect.model_trainer.entrypoint import main

configure_logging()
main()
