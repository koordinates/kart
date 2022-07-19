from kart.docs import ManPageWriter


def test_simple_man_page():
    """
    Test creating most simple man page
    """
    man = ManPageWriter("my-command")
    man.short_help = "Command to test man pages for click."
    man.date = "00-Jan-0000"
    man_content = '.TH "MY-COMMAND" "1" "00-Jan-0000" "1.0.0" "my-command Manual"\n.SH NAME\nmy-command \\- Command to test man pages for click.\n.SH SYNOPSIS\n.B my-command\n'
    assert str(man) == man_content


def test_full_man_page():
    """
    Test creating man page with all options set
    """
    man = ManPageWriter("my-command")
    man.short_help = "Command to test man pages for click."
    man.date = "00-Jan-0000"
    man.synopsis = "[--option1] [--option2]"
    man.description = """This is a

multi line description of a kart test."""

    man.options = [
        ("--option1", "Description for option1"),
        ("--option2", "Description for option2"),
    ]
    man.commands = [
        ("start", "Start it"),
        ("stop", "Stop it"),
        ("test", "Test it"),
    ]

    man_contents = '.TH "MY-COMMAND" "1" "00-Jan-0000" "1.0.0" "my-command Manual"\n.SH NAME\nmy-command \\- Command to test man pages for click.\n.SH SYNOPSIS\n.B my-command\n[\\-\\-option1] [\\-\\-option2]\n.SH DESCRIPTION\nThis is a\n.PP\nmulti line description of a kart test.\n.SH OPTIONS\n.TP\n\\fB\\-\\-option1\\fP \nDescription for option1\n.TP\n\\fB\\-\\-option2\\fP \nDescription for option2\n.SH COMMANDS\n.PP\n\\fBstart\\fP\n  Start it\n  See \\fBmy-command-start(1)\\fP for full documentation on the \\fBstart\\fP command.\n.PP\n\\fBstop\\fP\n  Stop it\n  See \\fBmy-command-stop(1)\\fP for full documentation on the \\fBstop\\fP command.\n.PP\n\\fBtest\\fP\n  Test it\n  See \\fBmy-command-test(1)\\fP for full documentation on the \\fBtest\\fP command.\n'

    assert str(man) == man_contents
