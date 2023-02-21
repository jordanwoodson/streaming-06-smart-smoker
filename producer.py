"""
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Jordan Woodson
    Date: FEB 15, 2023

"""

import pickle
import pika
import sys
import webbrowser
import csv
import time as sleep
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: tuple):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges

        msg=pickle.dumps(message)

        #this is where we decide our message gets sent based on the que name
        if(queue_name == '01-smoker'):
            ch.queue_declare(queue="01-smoker", durable=True)
            ch.basic_publish(exchange='', routing_key="01-smoker", body=msg, properties=pika.BasicProperties(delivery_mode=2))

        if(queue_name == '02-food-A'):
            ch.queue_declare(queue="02-food-A", durable=True)
            ch.basic_publish(exchange='', routing_key="02-food-A", body=msg, properties=pika.BasicProperties(delivery_mode=2))

        if(queue_name == '03-food-B'):
            ch.queue_declare(queue="03-food-B", durable=True)
            ch.basic_publish(exchange='', routing_key="03-food-B", body=msg, properties=pika.BasicProperties(delivery_mode=2))


        
        
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        
        conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes
    # message = " ".join(sys.argv[1:]) or "Second task....."
    # Read data from fil


with open('smoker-temps.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    
    fields=next(reader)
    for row in reader:
        i=0
        # i = time
        # i+1 = smoker
        # 1+2 = food a
        # i+3 = food b

        time=row[i]
        smoker_temp = row[i+1]
        if smoker_temp != "":
            print("[x] Sent smoker temp " + smoker_temp + ' to 01-smoker at ' + time)
            message1 = (time, smoker_temp)
            send_message("localhost","01-smoker",message1)

        # Publish food A temp to queue
        food_A_temp = row[i+2]
        if food_A_temp != "":
            print("[x] Sent food A temp " + food_A_temp + ' to 02-food-A at ' + time)
            message2 = (time, food_A_temp)
            send_message("localhost","02-food-A",message2)

        # Publish food B temp to queue
        food_B_temp = row[i+3]
        if food_B_temp != "":
            print("[x] Sent food B temp " + food_B_temp + ' to 03-food-B at ' + time)
            message3 = (time, food_B_temp)
            send_message("localhost","03-food-B",message3)

        # Wait for half a minute
        sleep.sleep(1)
