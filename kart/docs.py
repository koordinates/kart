"""
This module provides functionality to write a man page from some given
information about a CLI application and is modified from click-man.

https://github.com/click-contrib/click-man/tree/3a0684c9d70f94a2abef08fb3f88449dab6aea98

The original click-man source is licensed under the MIT License. Changes are
licensed under the Kart license (GPL).
"""

import os
import sys
import time
import typing as t

import click

COMMANDS_FOLDER = os.path.join(os.path.dirname(sys.prefix), "docs", "pages", "commands")


def get_short_help_str(command, limit=45):
    """
    Gets short help for the command or makes it by shortening the long help string.
    """
    return (
        command.short_help
        or command.help
        and click.utils.make_default_short_help(command.help, limit)
        or ""
    )


class Doc:
    TITLE = ""
    SECTION = ""
    PARAGRAPH = ""
    BOLD = ""
    INDENT = ""

    def __init__(self, ctx: click.Context, doc: t.Optional[str] = None) -> None:
        from kart.cli import get_version

        self.command = ctx.command_path
        self.version = get_version()
        self.short_help = get_short_help_str(ctx.command)
        self.description = ctx.command.help
        self.synopsis = " ".join(ctx.command.collect_usage_pieces(ctx))
        self.options = list(
            filter(None, (p.get_help_record(ctx) for p in ctx.command.params))
        )
        self.date = time.strftime("%Y-%m-%d")
        self.commands = []
        commands = getattr(ctx.command, "commands", None)
        if commands:
            self.commands = [(k, get_short_help_str(v)) for k, v in commands.items()]
        if doc:
            self.doc = doc


class ManPage(Doc):
    TITLE = ".TH"
    SECTION = ".SH"
    PARAGRAPH = ".PP"
    BOLD = ".B"
    INDENT = ".TP"


class TextPage(Doc):
    TITLE = "^"
    SECTION = "*"
    INDENT = " " * 3


class BaseDocWriter:
    def __init__(self, doc: t.Type[Doc]) -> None:
        self.doc = doc
        self.output = ""

    @classmethod
    def get_doc_writer(cls, doc: t.Type[Doc]) -> "t.Type[BaseDocWriter]":
        if type(doc) == ManPage:
            return ManPageWriter(doc)
        elif type(doc) == TextPage:
            return TextPageWriter(doc)
        return BaseDocWriter(doc)

    def write(self) -> str:
        """Generate string representation of 'Doc'

        Raises:
            NotImplementedError: BaseDocWriter cannot write a doc

        Returns:
            str:  A generated string representation of 'Doc'
        """
        raise NotImplementedError("BaseDocWriter cannot write docs")

    def replace_blank_lines(self, s):
        """Find any blank lines and replace them"""
        if not s:
            return s
        lines = (self.doc.PARAGRAPH if l == "" else l for l in s.splitlines())
        return "\n".join(lines)


class ManPageWriter(BaseDocWriter):
    def format_section(self, section):
        return f"{self.doc.SECTION} {section}"

    def get_title(self):
        return f'{self.doc.TITLE} "{self.doc.command.upper()}" "1" "{self.doc.date}" "{self.doc.version}" "{self.doc.command} Manual"'

    def get_name(self):
        section = self.format_section("NAME")
        formatted_command = self.doc.command.replace(" ", r"\-")
        section_body = rf"{formatted_command} \- {self.doc.short_help}"
        return section + "\n" + section_body

    def get_synopsis(self):
        section = self.format_section("SYNOPSIS")
        section_body = (
            rf"{self.doc.BOLD} {self.doc.command}"
            + "\n"
            + self.doc.synopsis.replace("-", r"\-")
        )
        return section + "\n" + section_body

    def get_description(self):
        if not self.doc.description:
            return
        section = self.format_section("DESCRIPTION")
        section_body = rf"{self.replace_blank_lines(self.doc.description)}"
        return section + "\n" + section_body

    def get_options(self):
        if not self.doc.options:
            return
        section = self.format_section("OPTIONS")
        section_body = []
        for option, description in self.doc.options:
            section_body.append(self.doc.INDENT)
            parts = option.replace("-", r"\-").split(maxsplit=1)
            name, desc = parts if len(parts) == 2 else (parts[0], "")
            section_body.append(rf"\fB{name}\fP {desc}")
            section_body.append(self.replace_blank_lines(description))
        return section + "\n" + "\n".join(section_body)

    def get_commands(self):
        if not self.doc.commands:
            return
        section = self.format_section("COMMANDS")
        section_body = []
        for name, description in self.doc.commands:
            section_body.append(self.doc.PARAGRAPH)
            section_body.append(r"\fB{0}\fP".format(name))
            section_body.append("  " + self.replace_blank_lines(description))
            section_body.append(
                rf"  See \fB{self.doc.command}-{name}(1)\fP for full documentation on the \fB{name}\fP command."
            )
        return section + "\n" + "\n".join(section_body)

    def write(self):
        title = self.get_title()
        name = self.get_name()
        synopsis = self.get_synopsis()
        description = self.get_description()
        options = self.get_options()
        commands = self.get_commands()
        doc = getattr(self.doc, "doc", None)
        man_page = "\n".join(
            filter(
                None,
                [
                    title,
                    name,
                    synopsis,
                    description,
                    options,
                    commands,
                    doc,
                ],
            )
        )
        return man_page


class TextPageWriter(BaseDocWriter):
    def get_title(self):
        title = f"{self.doc.command.upper()} (1)    {self.doc.command}     Manual"
        return title + "\n" + self.doc.TITLE * len(title) + "\n"

    def format_section(self, section):
        formatted_section = f"{section}\n" + self.doc.SECTION * len(section) + "\n\t"
        return formatted_section

    def get_name(self):
        section = self.format_section("NAME")
        section_body = f'{self.doc.command.replace(" ", "-")}  {self.doc.short_help}'
        return section + section_body + "\n"

    def get_synopsis(self):
        section = self.format_section("SYNOPSIS")
        section_body = f'{self.doc.command} {self.doc.synopsis.replace("-", r"-")}'
        return section + section_body + "\n"

    def get_description(self):
        if not self.doc.description:
            return
        section = self.format_section("DESCRIPTION")
        section_body = self.replace_blank_lines(self.doc.description)
        return section + section_body + "\n"

    def get_options(self):
        if not self.doc.options:
            return
        section = self.format_section("OPTIONS")
        section_body = []
        for option, description in self.doc.options:
            section_body.append(self.doc.INDENT)
            parts = option.split(maxsplit=1)
            name, desc = parts if len(parts) == 2 else (parts[0], "")
            section_body.append(f"\t{name} {desc}")
            section_body.append(f"\t{self.replace_blank_lines(description)}")
        return section + "\n".join(section_body) + "\n"

    def get_commands(self):
        if not self.doc.commands:
            return
        section = self.format_section("COMMANDS")
        section_body = []
        for name, description in self.doc.commands:
            section_body.append(f"\t{self.doc.PARAGRAPH}")
            section_body.append(f"\t{name}")
            section_body.append(f"\t{self.replace_blank_lines(description)}")
            section_body.append(
                f"\tSee {self.doc.command}-{name}(1) for full documentation on the {name} command."
            )
        return section + "\n".join(section_body) + "\n" if section_body else None

    def get_footer(self):
        footer = f"{self.doc.version}   {self.doc.date}   {self.doc.command}"
        return footer + "\n"

    def write(self):
        title = self.get_title()
        name = self.get_name()
        synopsis = self.get_synopsis()
        description = self.get_description()
        options = self.get_options()
        commands = self.get_commands()
        footer = self.get_footer()
        doc = getattr(self.doc, "doc", None)
        text_page = "\n".join(
            filter(
                None,
                [
                    title,
                    name,
                    synopsis,
                    description,
                    options,
                    commands,
                    doc,
                    footer,
                ],
            )
        )
        return text_page
