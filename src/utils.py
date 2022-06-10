import ebs_msg_pb2

CONDITION_OP_STR = {
    ebs_msg_pb2.Condition.Operator.EQ: "=",
    ebs_msg_pb2.Condition.Operator.NE: "!=",
    ebs_msg_pb2.Condition.Operator.GT: ">",
    ebs_msg_pb2.Condition.Operator.GE: ">=",
    ebs_msg_pb2.Condition.Operator.LT: "<",
    ebs_msg_pb2.Condition.Operator.LE: "<=",
}

CONNECT_SRC_TYPE_STR = {
    # ebs_msg_pb2.Connect.SrcType.UNKNOWN: "Unknown",
    ebs_msg_pb2.Connect.SrcType.SUBSCRIBER: "Subscriber",
    ebs_msg_pb2.Connect.SrcType.PUBLISHER: "Publisher",
    ebs_msg_pb2.Connect.SrcType.BROKER: "Broker"
}


def get_str_connect_str_type(type: ebs_msg_pb2.Connect.SrcType) -> str:
    return CONNECT_SRC_TYPE_STR.get(type, 'Client')


def get_str_condition_op(op: ebs_msg_pb2.Condition.Operator) -> str:
    return CONDITION_OP_STR.get(op, " ")


def get_str_condition(cond: ebs_msg_pb2.Condition) -> str:
    return '{field:} {op:} {value:}'.format(field=cond.field, op=get_str_condition_op(cond.op), value=cond.value)


def get_str_subscription(sub: ebs_msg_pb2.Subscription) -> str:
    return ', '.join(get_str_condition(cond) for cond in sub.condition).strip()


def get_str_publication(pub: ebs_msg_pb2.Publication) -> str:
    return 'company={company:}, value={value:}, drop={drop:}, variation={variation:}, date={date:}'.format(
                company=pub.company,
                value=pub.value,
                drop=pub.drop,
                variation=pub.variation,
                date=pub.date
    )