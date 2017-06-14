from kafka import KafkaProducer
import random
import time
import sys, os
import logging
import re

def _get_epoch(line_splitted):
    try:
        this_tweet_date = line_splitted[7]
        this_tweet_epoch = time.mktime(time.strptime(this_tweet_date, "%d/%m/%Y %H:%M"))
    except ValueError:
        found = False

        i = 7

        while not found and i < len(line_splitted):
            i += 1
            found = re.match('2./03/2014 ..:..', line_splitted[i]) != None

        this_tweet_date = line_splitted[i]
        this_tweet_epoch = time.mktime(time.strptime(this_tweet_date, "%d/%m/%Y %H:%M"))    

    return this_tweet_epoch

if __name__== '__main__':
    logging.basicConfig(level=logging.DEBUG)
    try:
        logging.info("Initialization...")
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        
        topic = sys.argv[1]
        filename = sys.argv[2]    
        
        logging.info("Sending messages to kafka '%s' topic..." % topic)
    
        with open(filename, 'rt') as f:
            try:
                next(f) # skip csv header

                line = next(f)

                while line != None:
                    logging.info(line)


                    try: 
                        next_line = next(f)

                        line_splitted = line.split('\t')

                        this_tweet_epoch = _get_epoch(line_splitted)

                        line_splitted_next = next_line.split('\t')

                        next_tweet_epoch = _get_epoch(line_splitted_next)

                        time_to_sleep = abs(next_tweet_epoch - this_tweet_epoch)

                        if time_to_sleep > 60:
                             time_to_sleep = random.uniform(float(2), float(20))
                        elif time_to_sleep == 0 or time_to_sleep == 60:
                            time_to_sleep = random.uniform(float(0.0), float(0.3))
                        
                        logging.info('Sleeping for %f seconds' % time_to_sleep)
                        producer.send(topic, bytes(line, 'utf8'))
                        time.sleep(time_to_sleep)                         

                        line = next_line
                    except StopIteration:
                        next_line = None       
            finally:
                f.close()
    
        logging.info("Waiting to complete delivery...")
        producer.flush()
        logging.info("End")

    except KeyboardInterrupt:
        logging.info('Interrupted from keyboard, shutdown')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)