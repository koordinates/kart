import contextlib
import os

import tqdm


@contextlib.contextmanager
def progress_bar(*args, show_progress=None, disable=None, **kwargs):
    """Returns a tqdm progress bar that closes automatically."""
    if show_progress is False:
        disable = True
    elif disable is None and os.environ.get("KART_SHOW_PROGRESS"):
        disable = False

    try:
        tqdm_progress_bar = tqdm.tqdm(*args, disable=disable, **kwargs)
        yield tqdm_progress_bar
    finally:
        tqdm_progress_bar.close()
