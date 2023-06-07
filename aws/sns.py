import logging
import boto3
from datetime import datetime
import pytz
from typing import Callable, Optional

class SNS:
    def __init__(self, 
                 subject:str,
                 message:str,
                 subject_writer:Optional[Callable],
                 message_writer:Optional[Callable],
                 topic_arn = None, 
                 region_name = 'us-east-1'):
        self.client = boto3.client('sns', region_name=region_name)
        self.topic_arn = topic_arn or 'arn:aws:sns:us-east-1:618572314333:DataBrew-Job-Completion'
        self.subject = subject
        self.message = message
        self.message_writer:Optional[Callable] = message_writer
        self.subject_writer:Optional[Callable] = subject_writer

    def get_time_message(self):
        dt = datetime.now(pytz.timezone('Australia/Sydney'))
        formatted_dt = dt.strftime('%H:%M @ %Y/%m/%d: ')
        return formatted_dt
 

    def write_message(self, input):
                
        if self.message_writer:
            message_to_add = self.message_writer(input)    
            if not isinstance(input, str):
                message_to_add = '\n\n'.join(message_to_add)
            return self.message.replace("{ }", message_to_add)
        else:
            return self.message
    
    def write_subject(self, input):
        if self.subject_writer:
            return self.subject.replace("{ }", self.subject_writer(input))
        else:
            return self.subject 


    def publish(self, input):
        message = self.write_message(input)
        subject = self.get_time_message() + self.write_subject(input)
        
        if isinstance(message, list):
            message = self.format_message(message)

        if message:
            logging.info("Information was published.")
            self.client.publish(
                TargetArn = self.topic_arn,
                Message = message,
                Subject = subject)
            
        logging.info(f"No information was published.")
        
    def format_message(self, messages: list) -> str:
        return "\n\n".join(messages)










