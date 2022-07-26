import pytest
import click
import os
import time

from kart.docs import BaseDocWriter, ManPage, TextPage
from kart.cli import get_version


@pytest.mark.parametrize(
    "doc, expected_output",
    [
        (
            ManPage,
            f'.TH "" "1" "{time.strftime("%Y-%m-%d")}" "{get_version()}" " Manual"\n.SH NAME\n \\- \n.SH SYNOPSIS\n.B \n[OPTIONS]\n.SH OPTIONS\n.TP\n\\fB+p\\fP \n\n.TP\n\\fB!e\\fP \n',
        ),
        (
            TextPage,
            f' (1)         Manual\n^^^^^^^^^^^^^^^^^^^\n\nNAME\n****\n\t  \n\nSYNOPSIS\n********\n\t [OPTIONS]\n\nOPTIONS\n*******\n\t   \n\t+p \n\t\n   \n\t!e \n\t\n\n{get_version()}   {time.strftime("%Y-%m-%d")}   \n',
        ),
    ],
)
def test_doc_page(doc, expected_output):
    @click.command()
    @click.option("+p", is_flag=True)
    @click.option("!e", is_flag=True)
    def test(p, e):
        pass

    ctx = click.Context(test)
    doc_page = doc(ctx)
    writer = BaseDocWriter.get_doc_writer(doc_page)
    output = writer.write()

    assert expected_output == output
