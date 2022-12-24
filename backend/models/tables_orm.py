from tortoise.models import Model
from tortoise import fields


class AggregatorORM(Model):
    name = fields.CharField(max_length=255, null=True, default="")
    username = fields.CharField(max_length=255, pk=True)
    email = fields.CharField(max_length=255)
    password = fields.CharField(max_length=255)
    mobile = fields.BigIntField(null=True)
    agents: fields.ReverseRelation['AgentORM']
    reports: fields.ReverseRelation['ReportORM']

    class Meta:
        table = "aggregators"
        unique_together = (('username', 'mobile'),)


class AgentORM(Model):
    id = fields.UUIDField(pk=True)
    agent_id = fields.IntField()
    name = fields.CharField(max_length=255)
    mobile = fields.BigIntField()
    aggregator: fields.ForeignKeyRelation = fields.ForeignKeyField('models.AggregatorORM', related_name='agents', on_delete="CASCADE")

    class Meta:
        table = "agents"


class ReportORM(Model):
    name = fields.CharField(max_length=255, pk=True)
    date = fields.DatetimeField(auto_now_add=True)
    url = fields.CharField(max_length=511, unique=True)
    aggregator: fields.ForeignKeyRelation = fields.ForeignKeyField('models.AggregatorORM', related_name='reports', on_delete="CASCADE")

    class Meta:
        table = "reports"
