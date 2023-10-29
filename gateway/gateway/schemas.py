from marshmallow import Schema, fields, post_dump


class CreateProductSchema(Schema):
    id = fields.Str(required=True)
    title = fields.Str(required=True)
    maximum_speed = fields.Int(required=True)
    in_stock = fields.Int(required=True)
    passenger_capacity = fields.Int(required=True)


class UpdateProductSchema(Schema):
    title = fields.Str(required=False, missing=None)
    maximum_speed = fields.Int(required=False, missing=None)
    in_stock = fields.Int(required=False, missing=None)
    passenger_capacity = fields.Int(required=False, missing=None)


class CreateOrderDetailSchema(Schema):
    product_id = fields.Str(required=True)
    price = fields.Decimal(as_string=True, required=True)
    quantity = fields.Int(required=True)


class CreateOrderSchema(Schema):
    order_details = fields.Nested(
        CreateOrderDetailSchema, many=True, required=True
    )


class GetOrderSchema(Schema):
    class OrderDetail(Schema):
        id = fields.Int()
        quantity = fields.Int()
        product_id = fields.Str()
        image = fields.Str()
        price = fields.Decimal(as_string=True)
        product = fields.Nested(CreateProductSchema, many=False)

    id = fields.Int()
    order_details = fields.Nested(OrderDetail, many=True)

    @post_dump(pass_many=True)
    def wrap_with_count(self, data, many, **kwargs):
        if many:
            return {"count": len(data), "data": data}
        return data
