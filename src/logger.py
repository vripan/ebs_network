import datetime, os, logging
from globals import LOG_CONFIG

def get_auxiliary_dir(name):
    return os.path.normpath(os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        name
    )))


def get_auxiliary_file(extension: str):
    filename = os.path.splitext(os.path.basename(__file__))[
        0] + "." + extension
    filepath = os.path.normpath(os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        filename
    )))

    return filename, filepath

def setup_logger():
    run_dir = LOG_CONFIG["directory"]
    level = LOG_CONFIG["level"]

    timestamp = int(datetime.datetime.now().timestamp())
    run_path = get_auxiliary_dir(run_dir)

    log_path = os.path.join(run_path, "%u.%08x.log" % (timestamp, os.getpid()))

    if not os.path.exists(run_path):
        os.mkdir(run_path)
    elif not os.path.isdir(run_path):
        raise Exception("path '%s' is not a directory")

    logging.basicConfig(
        level=level,
    )

    # log_formatter = logging.Formatter(
    #     fmt='%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
    #     datefmt='%Y-%m-%d %H:%M:%S'
    # )

    log_formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    log_file_handle = logging.FileHandler(
        log_path, mode="wt", encoding='utf-8')
    log_file_handle.setFormatter(log_formatter)

    root_logger = logging.getLogger()
    root_logger.handlers[0].setFormatter(log_formatter)
    root_logger.handlers[0].addFilter(logging.Filter("root"))

    root_logger.addHandler(log_file_handle)

    logging.info(f"Logging to {log_path}")
