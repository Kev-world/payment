from main import redis, Order
import time

key = 'refund_order'
group = 'payment-group'

try:
    # create consumer group
    redis.xgroup_create(key, group)
except:
    print('Group Already Exists!')

while True:
    try:
        # consume the redis stread
        results = redis.xreadgroup(group, key, {key: '>'}, None)

        if results != []:
            print(results)
            for result in results:
                obj = result[1][0][1]
                order = Order.get(obj['pk'])
                order.status = 'refunded'
                order.save()
               
    except Exception as e:
        print(str(e))
    time.sleep(1)