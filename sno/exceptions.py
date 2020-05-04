import click

# Exit codes

SUCCESS = 0
SUCCESS_WITH_FLAG = 1

INVALID_ARGUMENT = 2

# We could use 1 for this, except in --exit-code mode.
# So we always use 11 for consistency.
UNCATEGORIZED_ERROR = 11

INVALID_OPERATION = 20
MERGE_CONFLICT = 21
PATCH_DOES_NOT_APPLY = 22

NOT_YET_IMPLEMENTED = 30

NOT_FOUND = 40
NO_REPOSITORY = 41
NO_DATA = 42
NO_BRANCH = 43
NO_CHANGES = 44
NO_WORKING_COPY = 45
NO_USER = 46
NO_COMMIT = 47
NO_IMPORT_SOURCE = 48
NO_TABLE = 49

SUBPROCESS_ERROR_FLAG = 128
DEFAULT_SUBPROCESS_ERROR = 129


class BaseException(click.ClickException):
    """
    A ClickException that can easily be constructed with any exit code,
    and which can also optionally give a hint about which param lead to
    the problem.
    Providing a param hint or not can be done for any type of error -
    an unparseable import path and an import path that points to a
    corrupted database might both provide the same hint, but be
    considered completely different types of errors.
    """

    exit_code = UNCATEGORIZED_ERROR

    def __init__(self, message, exit_code=None, param=None, param_hint=None):
        super(BaseException, self).__init__(message)

        if exit_code is not None:
            self.exit_code = exit_code

        self.param_hint = None
        if param_hint is not None:
            self.param_hint = param_hint
        elif param is not None:
            self.param_hint = param.get_error_hint(None)

    def format_message(self):
        if self.param_hint is not None:
            return f"Invalid value for {self.param_hint}: {self.message}"
        return self.message


class InvalidOperation(BaseException):
    exit_code = INVALID_OPERATION


class NotYetImplemented(BaseException):
    exit_code = NOT_YET_IMPLEMENTED


class NotFound(BaseException):
    exit_code = NOT_FOUND


class SubprocessError(BaseException):
    exit_code = DEFAULT_SUBPROCESS_ERROR

    def __init__(
        self,
        message,
        exit_code=None,
        param=None,
        param_hint=None,
        called_process_error=None,
    ):
        super(SubprocessError, self).__init__(
            message, exit_code=exit_code, param=param, param_hint=param_hint
        )
        if called_process_error and not exit_code:
            self.exit_code = SUBPROCESS_ERROR_FLAG + called_process_error.return_code
