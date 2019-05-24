'''
passar + un param: https://github.com/JosepSampe/pywren-ibm-cloud/blob/master/examples/multiple_parameters_call_async.py
'''
import pywren_ibm_cloud as pywren
import random
import pika
import os
import sys
import json

comptador = None
v = None
resultat = []
global id

def master(N):
    '''
    :N Numero total de maps
    '''
    global comptador
    global v
    v=0
    comptador=0
    pw_config = json.loads(os.environ.get('PYWREN_CONFIG', ''))
    urlAMQP=pw_config['rabbitmq']['amqp_url']
    params=pika.URLParameters(urlAMQP)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare('cua'+str(000))
    channel.queue_bind(exchange='logs',queue='cua'+str(000))
    v=(int)(random.random() * N + 1)
    channel.basic_publish(exchange='', routing_key='cua'+str(v), body='start')
    print(f'master msg send {v}')
    def callback(ch, method, properties, body):
        bodyr = body.decode('latin1')
        print(f'master received {bodyr}')
        global comptador
        global v
        comptador=comptador+1
        if comptador == N:
            channel.basic_publish(exchange='logs', routing_key='', body='stop')
            print('master msg sent stop')
            channel.stop_consuming()
        else:
            v=(int)(random.random() * N + 1)
            print(v)
            channel.basic_publish(exchange='', routing_key='cua'+str(v), body='start')
            print(f'master msg sent start {v}')
    channel.basic_consume(callback, queue='cua'+str(000), no_ack=True)
    channel.start_consuming()
    channel.close()

def slave(identificador): 
    '''
    :identificador id de la funcio
    '''
    global v
    global resultat
    global id
    id = identificador
    resultat=[]
    v=0
    pw_config = json.loads(os.environ.get('PYWREN_CONFIG', ''))
    urlAMQP=pw_config['rabbitmq']['amqp_url']
    params=pika.URLParameters(urlAMQP)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare('cua'+str(identificador)+'a')
    channel.queue_bind(exchange='logs',queue='cua'+str(identificador)+'a')
    
       
    def callback(ch, method, properties, body):
        global v
        global resultat
        global id
        primer_missatge=body.decode("latin1")
        print(f'msg: {primer_missatge}')
        if primer_missatge == 'start':
            #soc el elegit           
            v=(int)(random.random() * 100 + 1)
            print(f'{id} sends {v}')
#             resultat.append(v)
            channel.basic_publish(exchange='logs', routing_key='', body=str(v))
        elif primer_missatge =='stop':
#             channel.basic_publish(exchange='logs', routing_key='', body='stop')
            channel.stop_consuming()
        elif str.isdigit(primer_missatge):
            print(f'{id} received {primer_missatge}')
            resultat.append(primer_missatge)
            
    channel.basic_consume(callback, queue='cua'+str(identificador)+'a', no_ack=True)
    channel.start_consuming()
    channel.close()
    return resultat

if __name__ == '__main__':
    if len(sys.argv) == 2:
        if int(sys.argv[1]) <= 0:
            print("S'ha detectat un valor negatiu o zero, no es pot fer res.")
            sys.exit(-1)
        else:
            num_esclaus=int(sys.argv[1])
            iterdata=[]
            i=0
            while i<num_esclaus:
                param=[str(i+1)]
                iterdata.append(param)
                i+=1
            pw = pywren.ibm_cf_executor(rabbitmq_monitor=True) #rabbitmq_monitor=True
            urlAMQP=pw.config['rabbitmq']['amqp_url']
            params=pika.URLParameters(urlAMQP)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.exchange_declare(exchange='logs', exchange_type='fanout')
            pw = pywren.ibm_cf_executor(rabbitmq_monitor=True) #rabbitmq_monitor=True
            
            
            pw.call_async(master, num_esclaus)

            pw=pywren.ibm_cf_executor(rabbitmq_monitor=True)
            pw.map(slave, iterdata)
            #pw.create_timeline_plots('path_on_vull_que_es_guardi', 'nom_fitxer')
            pw.monitor()
            print(pw.get_result())
    else:
        print("USAGE: map.py <num_maps>") 