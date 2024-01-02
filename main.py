from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.background import BackgroundTasks
from redis_om import get_redis_connection, HashModel
from starlette.requests import Request
import requests, time
from dotenv import load_dotenv
import os

app = FastAPI()

load_dotenv()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

host = os.getenv('host')
port = os.getenv('port')
password = os.getenv('password')


# Should be different database
redis = get_redis_connection(
    host=host,
    port=port,
    password=password,
    decode_responses=True
)

class Order(HashModel):
    product_id: str
    price: float
    fee: float
    total: float
    quantity: int
    status: str # pending, completed, refunded

    class Meta:
        database = redis

@app.get('/orders/{pk}')
async def get_one(pk: str):
    return Order.get(pk)

@app.post('/orders')
async def create(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()

    req = requests.get('http://localhost:8000/products/%s' % body['id'])
    product = req.json()

    order = Order(
        product_id=body['id'],
        price=product['price'],
        fee=0.2 * product['price'],
        total=1.2 * product['price'],
        quantity=body['quantity'],
        status='pending'
    )
    order.save()

    background_tasks.add_task(order_completed, order)

    return order

def order_completed(order: Order):
    time.sleep(5)
    order.status = 'completed'
    order.save()
    # send event to other MS via redis stream
    redis.xadd('order_completed', order.dict(), '*')
