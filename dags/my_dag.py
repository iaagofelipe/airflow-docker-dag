from email import message
from multiprocessing.managers import ValueProxy
from urllib.request import urlopen
from datetime import date, datetime, timedelta
import re
from xmlrpc.client import DateTime
from airflow.models.dag import ScheduleInterval
from jinja2.runtime import Context
import requests
import logging

import airflow
from airflow import DAG
from airflow.models import Variable, taskinstance
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import smtplib, ssl

from sqlalchemy.sql.expression import true

start_date_dag = airflow.utils.dates.days_ago(1)
# start_date_dag = datetime.now();

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date_dag
}

MAX_TIMEOUT = timedelta(minutes=10)
important = []
keep_phrases = ["Finalizando importacao lote"]
url =r'http://10.85.20.8/maj/rsfilemanager.log'
# http://cpisefaz.mobitbrasil.com.br/logs_mobit/rscontrol/rsfilemanager.log
# http://10.85.20.8/maj/rsfilemanager.log

#TODO armazenar estado de falha e usar essa informação para controlar o envio de email.

SEND_EMAIL = str(Variable.get('SEND_EMAIL', 'False')) == 'True'
EMAIL_RECEIPIENTS = Variable.get('EMAIL_RECEIPIENTS', '')

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
  message = '''
    Subject: Erro finalizando importacao
      ERROR - tempo entre ultimo log e data atual maior que 10 minutos.'''

  with smtplib.SMTP(smtp_server, port) as server:
    logging.info("FAZENDO LOGIN NO SERVIDOR")    
    server.starttls(context=context)
    server.login(sender_email, password)
    server.sendmail(
      sender_email,
      "iagosilva@mobitbrasil.com.br",
      message) 
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
      status_importacao = Variable.set_val("status_importacao", "Erro")
    else:
      status_importacao = Variable.get("status_importacao")
  except RuntimeError as err:
    logging.error('Não foi possível validar o tempo de finalização de importação para o lote', err)    

show_results(MAX_TIMEOUT, log_text, date_time_str, date_time_difference)

def execute_dag():
  file = search_file()
  log_text, date_time_str, date_time_difference = scrap_file(important, keep_phrases, file)
  show_results(MAX_TIMEOUT, log_text, date_time_str, date_time_difference)

with DAG(
    default_args=args,
    dag_id='verificacao_log',
    tags=['verificação'],
    catchup=False,
    start_date=start_date_dag,
    schedule_interval = timedelta(minutes=10)
) as dag:
  task1 = PythonOperator(
    
        task_id='execute_dag',
        python_callable=execute_dag,
        email_on_failure=SEND_EMAIL,
        email=[EMAIL_RECEIPIENTS],
        retries=0,
        dag=dag
    )

task1 