import json
from http import HTTPStatus

from marshmallow import ValidationError
from nameko import config
from nameko.exceptions import BadRequest
from nameko.rpc import RpcProxy
from werkzeug import Response

from gateway.entrypoints import http
from gateway.exceptions import OrderNotFound, ProductNotFound, ProductExists
from gateway.schemas import CreateOrderSchema, GetOrderSchema, CreateProductSchema, UpdateProductSchema


class GatewayService(object):
    """
    Service acts as a gateway to other services over http.
    """

    name = 'gateway'

    orders_rpc = RpcProxy('orders')
    products_rpc = RpcProxy('products')

    @http(
        "GET", "/products/<string:product_id>",
        expected_exceptions=ProductNotFound
    )
    def get_product(self, request, product_id):
        """Gets product by `product_id`
        """
        product = self.products_rpc.get(product_id)
        return Response(
            CreateProductSchema().dumps(product).data,
            status=HTTPStatus.OK,
            mimetype='application/json'
        )

    @http(
        "POST", "/products",
        expected_exceptions=(ValidationError, BadRequest, ProductExists)
    )
    def create_product(self, request):
        """Create a new product - product data is posted as json

        Example request ::

            {
                "id": "the_odyssey",
                "title": "The Odyssey",
                "passenger_capacity": 101,
                "maximum_speed": 5,
                "in_stock": 10
            }


        The response contains the new product ID in a json document ::

            {"id": "the_odyssey"}

        Throws ProductExists exception when product ID already exists

        """

        schema = CreateProductSchema(strict=True)

        try:
            # load input data through a schema (for validation)
            # Note - this may raise `ValueError` for invalid json,
            # or `ValidationError` if data is invalid.
            product_data = schema.loads(request.get_data(as_text=True)).data
        except ValueError as exc:
            raise BadRequest("Invalid json: {}".format(exc))

        # Create the product
        self.products_rpc.create(product_data)
        return Response(
            json.dumps({'id': product_data['id']}),
            status=HTTPStatus.CREATED,
            mimetype='application/json'
        )

    @http(
        "PATCH", "/products/<string:product_id>",
        expected_exceptions=(ValidationError, BadRequest, ProductNotFound)
    )
    def update_product(self, request, product_id):
        """Updates an existing product - product data is posted as json

        Example request ::

            {
                "id": "the_odyssey",
                "title": "The New Odyssey"
            }


        The response contains the updated product ID in a json document ::

            {"id": "the_odyssey"}

        Throws ProductNotFound exception when product doesn't exists

        """

        schema = UpdateProductSchema(strict=True)

        try:
            # load input data through a schema (for validation)
            # Note - this may raise `ValueError` for invalid json,
            # or `ValidationError` if data is invalid.
            # Note that not all fields are required, only those
            # that we want to update
            product_data = schema.loads(request.get_data(as_text=True)).data
        except ValueError as exc:
            raise BadRequest("Invalid json: {}".format(exc))

        # Updates the product
        self.products_rpc.update(product_id, product_data)
        return Response(
            json.dumps({'id': product_id}),
            status=HTTPStatus.OK,
            mimetype='application/json'
        )

    @http(
        "DELETE", "/products/<string:product_id>",
        expected_exceptions=ProductNotFound
    )
    def delete_product(self, request, product_id):
        """Deletes product by `product_id`
        """

        self.products_rpc.delete(product_id)

        return Response(
            json.dumps({'deleted_id': product_id}),
            status=HTTPStatus.OK,
            mimetype='application/json'
        )

    @http("GET", "/orders/<int:order_id>", expected_exceptions=OrderNotFound)
    def get_order(self, request, order_id):
        """Gets the order details for the order given by `order_id`.

        Enhances the order details with full product details from the
        products-service.
        """
        order = self._get_order(order_id)
        return Response(
            GetOrderSchema().dumps(order).data,
            status=HTTPStatus.OK,
            mimetype='application/json'
        )

    def _get_order(self, order_id):
        # Retrieve order data from the orders service.
        # Note - this may raise a remote exception that has been mapped to
        # raise``OrderNotFound``
        order = self.orders_rpc.get_order(order_id)

        # get the configured image root
        image_root = config['PRODUCT_IMAGE_ROOT']

        # Enhance order details with product and image details.
        for item in order['order_details']:
            product_id = item['product_id']

            # Fetch product from storage
            item['product'] = self.products_rpc.get(product_id)
            # Construct an image url.
            item['image'] = '{}/{}.jpg'.format(image_root, product_id)

        return order

    @http("GET", "/orders/all")
    def list_orders(self, request):
        """Gets all orders.
        """

        # For pagination
        page = int(request.args.get('p', 1))
        per_page = int(request.args.get('per_page', 10))

        orders = self.orders_rpc.list_orders(page, per_page)
        return Response(
            GetOrderSchema(many=True).dumps(orders).data,
            status=HTTPStatus.OK,
            mimetype='application/json'
        )

    @http(
        "POST", "/orders",
        expected_exceptions=(ValidationError, ProductNotFound, BadRequest)
    )
    def create_order(self, request):
        """Create a new order - order data is posted as json

        Example request ::

            {
                "order_details": [
                    {
                        "product_id": "the_odyssey",
                        "price": "99.99",
                        "quantity": 1
                    },
                    {
                        "price": "5.99",
                        "product_id": "the_enigma",
                        "quantity": 2
                    },
                ]
            }


        The response contains the new order ID in a json document ::

            {"id": 1234}

        """

        schema = CreateOrderSchema(strict=True)

        try:
            # load input data through a schema (for validation)
            # Note - this may raise `ValueError` for invalid json,
            # or `ValidationError` if data is invalid.
            order_data = schema.loads(request.get_data(as_text=True)).data
        except ValueError as exc:
            raise BadRequest("Invalid json: {}".format(exc))

        # Create the order
        # Note - this may raise `ProductNotFound`
        id_ = self._create_order(order_data)
        return Response(
            json.dumps({'id': id_}),
            status=HTTPStatus.CREATED,
            mimetype='application/json')

    def _create_order(self, order_data):
        # check order product ids are valid
        for item in order_data['order_details']:
            if not self.products_rpc.exists(item['product_id']):
                raise ProductNotFound(
                    "Product Id {}".format(item['product_id'])
                )

        # Call orders-service to create the order.
        # Dump the data through the schema to ensure the values are serialized
        # correctly.
        serialized_data = CreateOrderSchema().dump(order_data).data
        result = self.orders_rpc.create_order(
            serialized_data['order_details']
        )
        return result['id']
