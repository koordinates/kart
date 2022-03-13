from __future__ import annotations

from .exceptions import InvalidOperation

_RESERVED_WINDOWS_FILENAMES = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    }
)


def _validate_dataset_path(path: str) -> None:
    """
    Checks that the given dataset path has no disallowed characters or path components.
    """
    # disallow ASCII control characters as well as a few printable characters
    # (mostly because they're disallowed in Windows filenames)
    # but also because allowing ':' would make filter-spec parsing ambiguous in the Kart CLI.
    control_chars = set(range(0, 0x20))
    try:
        path_bytes = set(path.encode("utf8"))
    except UnicodeEncodeError:
        raise InvalidOperation(f"Dataset path {path!r} cannot be encoded using UTF-8")
    if not path_bytes:
        raise InvalidOperation(f"Dataset path {path!r} may not be empty")

    if path_bytes.intersection(control_chars):
        raise InvalidOperation(
            f"Dataset path {path!r} may not contain ASCII control characters"
        )
    other = ':<>"|?*'
    if set(path_bytes).intersection(other.encode()):
        raise InvalidOperation(
            f"Dataset path {path!r} may not contain any of these characters: {other}"
        )

    if path.startswith("/"):
        raise InvalidOperation(f"Dataset path {path!r} may not start with a '/'")

    components = path.upper().split("/")
    if any(not c for c in components):
        raise InvalidOperation(
            f"Dataset path {path!r} may not contain empty components"
        )

    bad_parts = sorted(_RESERVED_WINDOWS_FILENAMES.intersection(components))
    if bad_parts:
        raise InvalidOperation(
            f"Dataset path {path!r} may not contain a component called {bad_parts[0]}"
        )

    if any(comp.startswith(".") or comp.endswith(".") for comp in components):
        raise InvalidOperation(
            f"Dataset path {path!r} may not contain a component starting or ending with a '.'"
        )
    if any(comp.endswith(" ") for comp in components):
        raise InvalidOperation(
            f"Dataset path {path!r} may not contain a component ending with a ' '"
        )


def validate_dataset_paths(paths: list[str]) -> None:
    existing_paths_lower = {}
    for path in paths:
        _validate_dataset_path(path)
        path_lower = path.casefold()
        if path_lower in existing_paths_lower:
            existing_path = existing_paths_lower[path_lower]
            raise InvalidOperation(
                f"Dataset path {path!r} conflicts with existing path {existing_path!r}"
            )
        existing_paths_lower[path_lower] = path
