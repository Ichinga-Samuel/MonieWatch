from typing import Optional
from logging import getLogger

from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from pydantic import EmailStr, BaseModel, HttpUrl

from .env import env

logger = getLogger()

config = ConnectionConfig(
    MAIL_USERNAME=env.MAIL_USERNAME,
    MAIL_PASSWORD=env.MAIL_PASSWORD,
    MAIL_FROM=env.MAIL_FROM,
    MAIL_SERVER="in-v3.mailjet.com",
    MAIL_FROM_NAME="MonieWatch",
    MAIL_PORT=587,
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True
)


class Email(BaseModel):
    subject: str
    fast_mail: FastMail = FastMail(config)
    recipients: list[EmailStr]
    subtype: str = "plain"
    body: Optional['str']

    class Config:
        arbitrary_types_allowed = True

    def create_message(self):
        return MessageSchema(
            subject=self.subject,
            recipients=self.recipients,
            subtype=self.subtype,
            body=self.body
        )

    async def send(self) -> bool:
        try:
            msg = self.create_message()
            await self.fast_mail.send_message(msg)
            return True
        except Exception as exe:
            logger.warning(f"{exe}: Unable to send Email")
            return False


class ReportEmail(Email):
    link: HttpUrl
    subject = "Report is Ready"
    name: str

    def __init__(self, **kwargs):
        super(ReportEmail, self).__init__(**kwargs)
        self.body = self.msg

    @property
    def msg(self):
        return f"""
        Dear {self.name},
        This is to inform you that your report is ready.
        {self.link}
        """
