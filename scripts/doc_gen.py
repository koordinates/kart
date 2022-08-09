"""
This module provides functionality to write a man page from some given
information about a CLI application and is modified from click-man.

https://github.com/click-contrib/click-man/tree/3a0684c9d70f94a2abef08fb3f88449dab6aea98

The original click-man source is licensed under the MIT License. Changes are
licensed under the Kart license (GPL).
"""

import os
import platform
from pathlib import Path
import time
import typing as t
from pkg_resources import iter_entry_points

import click
import rst2txt
from docutils.core import publish_string
from docutils.writers import manpage

COMMANDS_FOLDER = Path.home() / os.path.join(
    "Documents", "gh", "kart", "docs", "pages", "commands"
)


def generate_help_pages(
    name: str,
    cli: click.Command,
    parent_ctx: click.Context = None,
    version: str = None,
    target_dir: str = None,
):
    ctx = click.Context(cli, info_name=name, parent=parent_ctx)
    doc_path = Path(COMMANDS_FOLDER) / f'{ctx.command_path.replace(" ", "-")}.rst'
    contents = ""
    if doc_path.exists():
        contents = doc_path.read_text()

    doc = get_parsed_doc(ctx, contents)
    writer = BaseDocWriter.get_doc_writer(doc)
    writer.write(target_dir)

    commands = getattr(cli, "commands", {})
    for name, command in commands.items():
        if command.hidden:
            continue
        generate_help_pages(
            name, command, parent_ctx=ctx, version=version, target_dir=target_dir
        )


def get_parsed_doc(ctx: click.Context, contents: str) -> "t.Type[Doc]":
    """Parses the click help text and rst docs"""
    if platform.system() == "Windows":
        converted_contents = rst_to_text(contents)
        doc = TextPage(ctx, converted_contents)
    else:
        converted_contents = rst_to_man(contents)
        doc = ManPage(ctx, converted_contents)
    return doc


def rst_to_man(contents):
    # we add a header to the man page and remove it later
    # to avoid parsing the rst2man "metadata" in ManPage
    header = "KART\n====\n"
    man_contents = publish_string(header + contents, writer=manpage.Writer()).decode(
        "utf-8"
    )[718:]
    return man_contents


def rst_to_text(contents):
    text_output = publish_string(contents, writer=rst2txt.Writer()).decode("utf-8")
    return text_output


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

    def write(self) -> None:
        """Generate string representation of 'Doc'

        Raises:
            NotImplementedError: BaseDocWriter cannot write a doc
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

    def write(self, target_dir: str = None):
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

        help_page = Path(f'{self.doc.command.replace(" ", "-")}.1')
        if target_dir:
            help_page = Path(target_dir) / help_page
        help_page.write_text(man_page)


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

    def write(self, target_dir: str = None):
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
        help_page = Path(f'{self.doc.command.replace(" ", "-")}')
        if target_dir:
            help_page = Path(target_dir) / help_page
        help_page.write_text(text_page)


if __name__ == "__main__":

    name = "kart"
    target = Path.home() / os.path.join(
        "Documents", "gh", "kart", "build", "venv", "help_page"
    )
    console_scripts = [ep for ep in iter_entry_points("console_scripts", name=name)]
    entry_point = console_scripts[0]
    target.mkdir(parents=True, exist_ok=True)
    import importlib

    mod = importlib.import_module(f"{entry_point.module_name}")

    from kart.cli import _load_all_commands, print_version

    _load_all_commands()
    cli = entry_point.resolve()

    # If the entry point isn't a click.Command object, try to find it in the module
    if not isinstance(cli, click.Command):
        from importlib import import_module
        from inspect import getmembers

        if not entry_point.module_name:
            raise click.ClickException('Could not find module name for "{name}".')
        ep_module = import_module(entry_point.module_name)
        ep_members = getmembers(ep_module, lambda x: isinstance(x, click.Command))

        ep_name, cli = ep_members[0]

    generate_help_pages(name, cli, version=entry_point.dist.version, target_dir=target)
