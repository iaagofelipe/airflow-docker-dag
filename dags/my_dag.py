from email import message
from urllib.request import urlopen
from datetime import datetime, timedelta
import re
from airflow.models.dag import ScheduleInterval
from jinja2.runtime import Context
import requests
import logging

import airflow
from airflow import DAG
from airflow.models import Variable, taskinstance
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

import smtplib, ssl

from sqlalchemy.sql.expression import true

start_date_dag = datetime.now();

MAX_TIMEOUT = timedelta(minutes=10)
important = []
keep_phrases = ["Finalizando importacao lote"]
url =r'http://10.85.20.8/maj/rsfilemanager.log'
# http://cpisefaz.mobitbrasil.com.br/logs_mobit/rscontrol/rsfilemanager.log
# http://10.85.20.8/maj/rsfilemanager.log

#TODO armazenar estado de falha e usar essa informação para controlar o envio de email.

def get_variable(var, default=None):
  try:
    return Variable.get(var, default)
  except:
    return default

SEND_EMAIL = str(get_variable('SEND EMAIL', 'False')) == 'True'
EMAIL_RECEIPIENTS = get_variable('EMAIL_RECEIPIENTS', '')


STATUS_IMPORTACAO = str(Variable.get("STATUS_IMPORTACAO", 'sucesso')) == 'erro'

def search_file():
    try:
      file = urlopen(url)
    except requests.Timeout as err:
      logging.error("Tempo excedido ", err)
    return file

file = search_file()

def scrap_file(important, keep_phrases, file):
  try:
    for line in file:
      for phrase in keep_phrases:
        if phrase in line.decode("utf-8"):
          important.append(line.decode("utf-8"))
          break

    log_text = important[len(important)-1]
    matches = re.findall(r'(\d+/\d+/\d+ \d+:\d+:\d+)',log_text)

    date_time_str = matches[0]
    date_time_obj = datetime.strptime(date_time_str, '%Y/%m/%d %H:%M:%S')

    date_time_difference =  datetime.today() - date_time_obj
    return log_text,date_time_str,date_time_difference
  except Exception as err:   
    logging.error("Não foi possível percorrer o arquivo ", err)

log_text, date_time_str, date_time_difference = scrap_file(important, keep_phrases, file)


def send_email_if_inconsistent_time():
  logging.info("tempo entre ultimo log e data atual maior que 10 minutos, alertando via e-mail")
  
  smtp_server = "smtp.office365.com"
  port = 587
  sender_email = "no-reply@mobitbrasil.com.br"
  password = "@M0b1t@M1t5"

  context = ssl.create_default_context()
  message = '''\
  From: no-reply@mobitbrasil.com.br
  Subject: Teste
  ERROR - tempo entre ultimo log e data atual maior que 10 minutos.
  '''

  try:
    with smtplib.SMTP(smtp_server, port) as server:
      server.starttls(context=context)
      server.login(sender_email, password)
      logging.debug("FAZENDO LOGIN NO SERVIDOR ")    
      server.sendmail(sender_email, EMAIL_RECEIPIENTS, (550,message)) 
  except Exception as e:
    logging.error("EMAIL NÃO ENVIADO " + str(e))    
  finally:
    logging.debug("FINALIZANDO SERVIDOR ")    
    server.quit() 

def show_results(MAX_TIMEOUT, log_text, date_time_str, date_time_difference):
  try:
    print('Data atual: ', datetime.today() )
    print('Dia do ultimo log: ',date_time_str)
    print('Tempo entre o ultimo log e a data atual: ', date_time_difference)
    print(log_text)
    inconsistent_time = date_time_difference >= MAX_TIMEOUT 
    if (inconsistent_time):
      send_email_if_inconsistent_time()
    
  except RuntimeError as err:
    logging.error('Não foi possível validar o tempo de finalização de importação para o lote', err)    

show_results(MAX_TIMEOUT, log_text, date_time_str, date_time_difference)

def execute_dag():
  file = search_file()
  log_text, date_time_str, date_time_difference = scrap_file(important, keep_phrases, file)
  show_results(MAX_TIMEOUT, log_text, date_time_str, date_time_difference)


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date_dag
}

dag = DAG(
    default_args=args,
    dag_id='verificacao_log',
    tags=['verificação'],
    start_date=start_date_dag,
    schedule_interval = MAX_TIMEOUT
)


task1 = PythonOperator(
      task_id='execute_dag',
      python_callable=execute_dag,
      email_on_failure=SEND_EMAIL,
      email=[EMAIL_RECEIPIENTS],
      retries=0,
      dag=dag
  )

task1 