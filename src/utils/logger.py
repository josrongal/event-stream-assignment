import logging


def get_logger(name):
    # Basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s',
        handlers=[logging.StreamHandler()]
    )

    return logging.getLogger(name)
