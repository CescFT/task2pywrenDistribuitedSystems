'''
Francesc Ferre Tarres
Eric Pi Esteban
Aleix Sancho Pujals

29/05/2019
'''
import pywren_ibm_cloud as pywren
import random
import pika
import os
import sys
import json

cont = None
random_number = None
result = []
global id

def master(total_slaves):
    global cont
    global random_number
    random_number = 0
    cont = 1
    pw_config = json.loads(os.environ.get('PYWREN_CONFIG', ''))
    urlAMQP=pw_config['rabbitmq']['amqp_url']
    params=pika.URLParameters(urlAMQP)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare('cua0')
    channel.queue_bind(exchange='logs',queue='cua0')
    random_number=(int)(random.random() * total_slaves + 1)
    channel.basic_publish(exchange='', routing_key=f'cua{random_number}', body='start')

    def callback(ch, method, properties, body):
        global cont
        global random_number
        i = 1
        if cont == total_slaves:
            while i <= total_slaves:
                channel.basic_publish(exchange='', routing_key=f'cua{i}', body='stop')
                i+=1
            channel.stop_consuming()
        else:
            if random_number >= total_slaves:
                random_number = 0
            random_number = random_number + 1
            channel.basic_publish(exchange='', routing_key=f'cua{random_number}', body='start')
        cont = cont+1
    channel.basic_consume(callback, queue='cua0', no_ack=True)
    channel.start_consuming()
    channel.close()

def slave(identifier):
    global random_number
    global result
    global id
    id = identifier
    result=[]
    random_number=0
    pw_config = json.loads(os.environ.get('PYWREN_CONFIG', ''))
    urlAMQP=pw_config['rabbitmq']['amqp_url']
    params=pika.URLParameters(urlAMQP)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(f'cua{identifier}')
    channel.queue_bind(exchange='logs', queue=f'cua{identifier}')
    
    def callback(ch, method, properties, body):
        global random_number
        global result
        global id
        message = body.decode("latin1")
        if message == 'start':       
            random_number=(int)(random.random() * 100 + 1)
            channel.basic_publish(exchange='logs', routing_key='', body=str(random_number))
        elif message == 'stop':
            channel.stop_consuming()
            channel.queue_delete(queue=f'cua{identifier}')
        else:
            print(f'{id} received {message}')
            result.append((int)(message))
            
    channel.basic_consume(callback, queue=f'cua{identifier}', no_ack=True)
    channel.start_consuming()
    channel.close()
    return result

if __name__ == '__main__':
    if len(sys.argv) == 2:
        if int(sys.argv[1]) <= 0:
            print("S'ha detectat un valor negatiu o zero, no es pot fer res.")
            sys.exit(-1)
        else:
            max_slaves = int(sys.argv[1])
            iterdata = []
            i = 0
            while i < max_slaves:
                param = [str(i+1)]
                iterdata.append(param)
                i = i + 1
            pw = pywren.ibm_cf_executor(rabbitmq_monitor=True) #rabbitmq_monitor=True
            urlAMQP = pw.config['rabbitmq']['amqp_url']
            params = pika.URLParameters(urlAMQP)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.exchange_declare(exchange='logs', exchange_type='fanout')
            pw=pywren.ibm_cf_executor(rabbitmq_monitor=True)
            pw.map(slave, iterdata)
            
            pw2 = pywren.ibm_cf_executor(rabbitmq_monitor=True) #rabbitmq_monitor=True
            pw2.call_async(master, max_slaves)

            pw.monitor()
            print(pw.get_result())
            channel.queue_delete(queue='cua0')
    else:
        print("USAGE: map.py <num_maps>")
