import asyncio
from typing import Sequence

from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph, Image, Table, TableStyle, PageTemplate, BaseDocTemplate, PageBreak, Spacer, Flowable
from reportlab.platypus.frames import Frame

from .dims import *


class DocBuilder:
    styles = getSampleStyleSheet()
    sub = ParagraphStyle(name='subtitle', alignment=1)
    body = ParagraphStyle(name='body', alignment=4)

    def __init__(self):
        self.data = []

    def add_header(self, *, heading: str, level: int = 1, **styles):
        style = ParagraphStyle(**styles) if styles else f"Heading{level}"
        self.data.append(Paragraph(heading, self.styles[style]))

    def add_title(self, *, title: str, **styles):
        style = ParagraphStyle(**styles) if styles else self.styles['Title']
        self.data.append(Paragraph(title, style))

    def add_body(self, *, body: str):
        self.data.append(Paragraph(body, self.body))

    def add_paragraph(self, *, body: str, style: ParagraphStyle = None, **styles):
        style = ParagraphStyle(**styles) if styles else (style or self.body)
        self.data.append(Paragraph(body, style))

    def add_table(self, *, data: Sequence[Sequence], styles: TableStyle = None, **kwargs):
        table = Table(data, **kwargs)
        table.setStyle(styles) if styles else None
        self.data.append(table)

    def add_flowable(self, *, flow: Flowable):
        self.data.append(flow)

    def add_page_break(self):
        self.data.append(PageBreak())

    def add_space(self, *, width: float, height: float):
        self.data.append(Spacer(width, height))

    @staticmethod
    def add_page_number(canvas, doc):
        canvas.saveState()
        canvas.setFont('Times-Roman', 10)
        canvas.drawCentredString(px(50), py(2), str(doc.page))
        canvas.restoreState()
