import asyncio
from functools import cache
from typing import Iterable
from pathlib import Path
from pydantic import BaseModel
from logging import getLogger

from utils.env import env
from utils.data_models import Transaction, Agent, Filter
from utils.pdf import BaseDocTemplate, dx, dy, px, py, PageTemplate, ParagraphStyle, DocBuilder, Frame, colors, TableStyle


logger = getLogger()


class BusinessSummary(BaseModel):
    business_name: str
    amount: float

    class Config:
        extra = "allow"


class Transactions(BaseModel):
    title: str
    transactions: Iterable[Transaction]
    target: float
    agents: list[Agent] = []
    filter: Filter = Filter()

    class Config:
        arbitrary_types_allowed = True

    def __hash__(self):
        return hash(self.title)

    def __iter__(self) -> Iterable[Transaction]:
        return self.transactions

    @property
    @cache
    def data(self) -> dict['str', BusinessSummary]:
        data_ = {}
        for trans in self:
            if not self.filter(trans=trans):
                continue
            key = trans.agent_id
            bus = data_.setdefault(key, {})
            if bus.get('business_name', None) is None:
                bus['business_name'] = trans.business_name
            bus['amount'] = (bus.get('amount') or 0) + trans.amount / 100
            bus[trans.trans_type] = (bus.get(trans.trans_type) or 0) + 1
        return {value['business_name']: BusinessSummary(**value) for key, value in data_.items()}

    @property
    @cache
    def sort_data(self) -> list:
        return sorted(self.data.keys(), reverse=True, key=lambda key: self.data[key].amount)

    def get_below_target_agents(self, target: float = 0) -> dict[str, BusinessSummary]:
        return {key: value for key, value in self.data.items() if value.amount < (target or self.target)}

    def table_data(self):
        keys = self.sort_data
        data = [[key, self.data[key].amount] for key in keys]
        data.insert(0, ['Business Name', "Amount"])
        return data

    def get_non_performing_agents(self):
        data = [[agent.name] for agent in self.agents if agent.name not in self.data.keys()]
        data.insert(0, ["Business Name"])
        return data

    async def get_pdf(self) -> Path | None:
        report = TransactionsReport(transactions=self)
        return await report.create()


class TransactionsReport(BaseDocTemplate):
    card_style = ParagraphStyle(name='card', borderColor=colors.darkorange, borderPadding=dx(3), borderRadius=5, borderWidth=1,
                                textColor=colors.darkgreen,
                                fontName="Helvetica", fontSize=12, spaceAfter=dx(3), autoLeading='max')
    card_text_format = """<para><b><font size=14>{business_name}</font></b><br/><br/>{details}<br/>{amount}</para>"""

    tabel_styles = TableStyle([("INNERGRID", (0, 0), (-1, -1), 1, colors.black), ("BOX", (0, 0), (-1, -1), 1, colors.black),
                         ("TEXTCOLOR", (1, 0), (1, -1), colors.darkorange), ("TEXTCOLOR", (0, 0), (0, -1), colors.darkgreen)])

    def __init__(self, *, transactions: Transactions, folder="reports", **kwargs):
        self.transactions = transactions
        self.file = env.BASE / f"{folder}/{self.transactions.title}.pdf"
        super().__init__(filename=str(self.file), leftMargin=dx(21), topMargin=dx(29.7), **kwargs)

        self.doc = DocBuilder()
        self.author = kwargs.get('author') or env.APP_NAME

        padding = dict(leftPadding=px(5), bottomPadding=px(5), rightPadding=px(5), topPadding=px(5))
        page_template = PageTemplate('normal', [Frame(0, 0, px(100), py(100), **padding, id='F1')], onPageEnd=self.doc.add_page_number)

        cover_template = PageTemplate('cover', [Frame(0, 0, px(100), py(100), id='F2')], onPage=self.add_author, autoNextPageTemplate=1)

        self.addPageTemplates([cover_template, page_template])

    def add_author(self, canvas, doc):
        canvas.saveState()
        canvas.setFont('Times-Roman', 18)
        canvas.drawCentredString(px(50), py(2), self.author)
        canvas.restoreState()

    def write_cover_page(self):
        self.doc.add_space(width=dx(5), height=dy(15))
        self.doc.add_title(title=self.transactions.title)
        self.doc.add_page_break()

    def write_table_of_transactions(self):
        data = self.transactions.table_data()
        if len(data) <= 1:
            return
        self.doc.add_title(title="Summary of Transactions")
        self.doc.add_table(data=data, styles=self.tabel_styles, repeatRows=1, spaceBefore=py(2))
        self.doc.add_page_break()

    def write_business_data(self, *, data: Iterable[BusinessSummary]):
        for business in data:
            details = """<br/>""".join(f"{' '.join(key.split('_')).title()}: {value}" for key, value in business.dict().items()
                                       if key != 'amount' and key != 'business_name')
            card_text = self.card_text_format
            text = card_text.format(business_name=business.business_name, details=details, amount=f"<strong>Total Amount: {business.amount}</strong>")
            self.doc.add_paragraph(body=text, style=self.card_style)
            self.doc.add_space(width=dx(3), height=dx(5))

    def write_below_target_performers(self):
        data = self.transactions.get_below_target_agents()
        if len(data) < 1:
            return
        title = "Agents Performing Below Target"
        self.doc.add_title(title=title)
        self.doc.add_space(width=dx(3), height=dx(3))
        self.write_business_data(data=data.values())
        self.doc.add_page_break()

    def write_agents_with_zero_transactions(self):
        data = self.transactions.get_non_performing_agents()
        if len(data) <= 1:
            return
        title = "Agents With No Transactions"
        self.doc.add_title(title=title)
        self.doc.add_space(width=dx(3), height=dx(3))
        self.doc.add_table(data=data, styles=self.tabel_styles, repeatRows=1, spaceBefore=py(2))
        self.doc.add_page_break()

    def write(self):
        self.write_cover_page()
        self.write_agents_with_zero_transactions()
        self.write_below_target_performers()
        self.write_table_of_transactions()

    async def create(self):
        try:
            self.write()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda flowables: self.build(flowables=flowables), self.doc.data)
            return self.file
        except Exception as err:
            logger.critical(f"{err}: Unable to create pdf report.")
