'''
passar + un param: https://github.com/JosepSampe/pywren-ibm-cloud/blob/master/examples/multiple_parameters_call_async.py
'''
import pywren_ibm_cloud as pywren
import random
import pika
import json, os
identificadors = 2

def tasca2_funcio(identificador, N): #identificador, N: Total valors
    pw_config=json.loads(os.environ.get('PYWREN_CONFIG',''))
    urlAMQP=pw_config['rabbitmq']['amqp_url']
    print(urlAMQP)
    resultat=[]
    if identificador == 0:
        params=pika.URLParameters(urlAMQP)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(exchange='logs', exchange_type='fanout')
        rand=(int)(random.random() * N + 1)
        message = "7"+str(rand)
        channel.basic_publish(exchange='logs', routing_key='', body=str(identificador))
        channel.basic_publish(exchange='logs', routing_key='', body=str(N))
        channel.basic_publish(exchange='logs', routing_key='', body="8")
        print(" [x] Sent %r" % message)
        connection.close()
    
    else:
        params=pika.URLParameters(urlAMQP)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        channel.exchange_declare(exchange='logs', exchange_type='fanout')
        
        channel.queue_declare('cua'+str(identificador)) #, exclusive=True
        queue_name = 'cua'+str(identificador)
        channel.queue_bind(exchange='logs', queue=queue_name)
        
        def callback(ch, method, properties, body):
            num=body.decode("latin1")
            num=int(num)
            resultat.append(num)
            if num == 8:
                channel.stop_consuming()
        
        channel.basic_consume(callback, queue=queue_name, no_ack=True) #, no_ack=True
        channel.start_consuming()
        #channel.start_consuming()
        connection.close()
    return resultat



v=list(range(identificadors)) #comensa per el identificador 0!!


iterdata=[]
i=0
while i<identificadors:
    param={'identificador':i, 'N':identificadors}
    iterdata.append(param)
    i+=1
print (iterdata)
input()
pw = pywren.ibm_cf_executor(rabbitmq_monitor=True) #rabbitmq_monitor=True
pw.map(tasca2_funcio, iterdata)
#pw.create_timeline_plots('path_on_vull_que_es_guardi', 'nom_fitxer')
pw.monitor()
print(pw.get_result())