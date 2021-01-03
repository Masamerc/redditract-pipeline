import datetime
import os
import praw
import pandas as pd
import logging as log

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from dataclasses import asdict
from pymongo import MongoClient

from reddit_extractor.util_classes import Comment, Submission, submission_factory, comment_factory
from reddit_extractor.util_funcs import convert_timestamp, extract_attributes_from_subreddit


# Mongo Setup
uri = Variable.get('mongo_uri')
client = MongoClient(uri)
db = client.redditract
subreddit_collec = db.subreddits
submission_collec = db.submissions


# date format for file names
date_asof = datetime.datetime.strftime(datetime.datetime.now(),"%m-%d-%Y")
date_file_format = datetime.datetime.strftime(datetime.datetime.now(),"%m%d%Y")


# Praw/Reddit Setup
reddit_credentials = Variable.get('reddit_credentials', deserialize_json=True)
REDDIT_KEY = reddit_credentials['reddit_key']
REDDIT_SECRET = reddit_credentials['reddit_secret']

reddit = praw.Reddit(
    client_id=REDDIT_KEY,
    client_secret=REDDIT_SECRET,
    user_agent="NA"
)

# email sender / receiver
EMAIL_RECEIVER = Variable.get('email_receiver')
EMAIL_SENDER = Variable.get('email_sender')


##################### for development - delete in production #####################
def branch_callable_for_dev():
    return Variable.get('start_task')
##################################################################################


def get_top100_subreddit_names(**context) -> list:
    """Scrape the subreddit names of top 100 subreddits """
    # read table from the webiste
    top_100 = pd.read_html('https://frontpagemetrics.com/top')
    top_100_subreddits = top_100[0]

    # return subreddit names as list 
    subreddit_names = [subreddit.split('/')[-1] for subreddit in top_100_subreddits.Reddit]

    context['ti'].xcom_push(key='subreddit_names', value=subreddit_names)
    
    return subreddit_names


def get_subreddit_data(**context) -> None:
    """Retrieve Subreddit data / details and store them to csv file"""

    subreddit_names = context['ti'].xcom_pull(key='subreddit_names', task_ids='get_top100_reddit_names')

    # get subreddit objects
    subreddits = [reddit.subreddit(subreddit_name) for subreddit_name in subreddit_names]

    # return attributes from each subreddit list of dict
    subreddit_data = [extract_attributes_from_subreddit(subreddit) for subreddit in subreddits]
 
    # store the result in dataframe
    subreddits_df = pd.DataFrame(subreddit_data)

    # add a few columns
    subreddits_df['created_date'] = subreddits_df.created.apply(convert_timestamp)
    subreddits_df['name'] = subreddit_names
    subreddits_df['asof'] = date_asof

    # save as csv
    subreddits_df.to_csv('/tmp/top_100_subreddits.csv', index=False)


def load_to_mongo(file_type: str, destination: str, **context) -> None:
    """Load data to Mongo DB"""
    df = pd.read_csv(f'/tmp/{file_type}.csv')
    
    if destination == 'subreddits':
        subreddit_collec.insert_many(df.to_dict(orient='records'))

    elif destination == 'submissions':
        submission_collec.insert_many(df.to_dict(orient='records'))
        
    else:
        log.critical('destination should be either submissions or subreddits')
        return

    
def get_submissions_data(category, num_subreddits=10, limit=10, **context):
    """Retieve Submission data / details and soter them to csv file"""

    subreddit_names = context['ti'].xcom_pull(key='subreddit_names', task_ids='get_top100_reddit_names')
    
    log.info(f'# of Subreddits: {len(subreddit_names)}')
    log.info(f'# of Submissions to retrieve: {limit}')
    
    submissions = []
    
    for subreddit in subreddit_names[:num_subreddits]:

        # submission_objects = get_submissions_from_subreddit(subreddit, category, limit)
        
        subreddit_obj = reddit.subreddit(subreddit)

        if category == 'hot':
            submission_obj = subreddit_obj.hot(limit=limit)
        elif category == 'new':
            submission_obj = subreddit_obj.new(limit=limit)
        else:
            submission_obj = subreddit_obj.top(limit=limit)
            
        submission_objects =  [sub for sub in submission_obj]

        
        submission_data = []
        for submission_obj in submission_objects:
            if submission_obj:
                submission_data.append(submission_factory(submission_obj, date_asof))

        for submission in submission_data:
            if submission:
                try:
                    submissions.append(asdict(submission))
                except TypeError as e:
                    print(submission)
                    print(e)
                    continue
    
    log.info('Data extraction complete.')
    
    pd.DataFrame(submissions).to_csv(f'/tmp/{category}_submissions.csv', index=False)

    

######################################### Airflow DAG #############################################

default_args = {
    'owner': 'redditract'
}

dag = DAG(
    'redditract-pipeline',
    start_date=days_ago(1),
    schedule_interval='@daily'
)

with dag:

    ############ branch for skipping downstreams - delete in production ########################
    # branch_task = BranchPythonOperator(
    #     task_id='branch',
    #     python_callable=branch_callable_for_dev
    # )
    ############################################################################################

    get_top100_subreddit_names_task = PythonOperator(
        task_id='get_top100_reddit_names',
        python_callable=get_top100_subreddit_names,
        provide_context=True
    )

    get_subreddit_data_task = PythonOperator(
        task_id='get_subreddit_data',
        python_callable=get_subreddit_data,
        provide_context=True
    )

    load_subreddit_to_mongo_task = PythonOperator(
        task_id='load_subreddit_to_mongo',
        python_callable=load_to_mongo,
        op_kwargs={'file_type': 'top_100_subreddits', 'destination': 'subreddits'},
        provide_context=True
    )

    store_hot_submission_to_csv_task = PythonOperator(
        task_id='store_hot_submission_to_csv',
        python_callable=get_submissions_data,
        op_kwargs={'category': 'hot', 'num_subreddits': 5, 'limit': 1},
        provide_context=True
    )

    store_top_submission_to_csv_task = PythonOperator(
        task_id='store_top_submission_to_csv',
        python_callable=get_submissions_data,
        op_kwargs={'category': 'top', 'num_subreddits': 5, 'limit': 1},
        provide_context=True
    )

    store_new_submission_to_csv_task = PythonOperator(
        task_id='store_new_submission_to_csv',
        python_callable=get_submissions_data,
        op_kwargs={'category': 'new', 'num_subreddits': 5, 'limit': 1},
        provide_context=True
    )

    load_hot_submission_to_mongo_task = PythonOperator(
        task_id='load_hot_submission_to_mongo',
        python_callable=load_to_mongo,
        op_kwargs={'file_type': 'hot_submissions', 'destination': 'submissions'},
        provide_context=True
    )

    load_new_submission_to_mongo_task = PythonOperator(
        task_id='load_new_submission_to_mongo',
        python_callable=load_to_mongo,
        op_kwargs={'file_type': 'new_submissions', 'destination': 'submissions'},
        provide_context=True
    )

    load_top_submission_to_mongo_task = PythonOperator(
        task_id='load_top_submission_to_mongo',
        python_callable=load_to_mongo,
        op_kwargs={'file_type': 'top_submissions', 'destination': 'submissions'},
        provide_context=True
    )

    send_email_task = EmailOperator(
        task_id='email_on_success',
        email_on_failure=True,
        email=EMAIL_SENDER,
        to=EMAIL_RECEIVER,
        subject='Redditractor Pipeline Ran Successfully: {{ ds }}',
        html_content='''

        Hello, this is an automated email from Redditractor, informing you of the successful pipeline-execution at {{ ds }}. <br>
        <br>
        <b>Result</b><br>
        -> Data has been loaded to Mongo Atlas Cluster<br>
        -> Subreddits: <br>
        <br>
        <table style="border: 1px solid black; width: 100%">
        <tr>
            <th style="border: 1px solid black;background-color: #368a67; color: white;">Subreddit Name</th>
        </tr>
        {% for subreddit in task_instance.xcom_pull(task_ids="get_top100_reddit_names", key="subreddit_names") %}
        <tr>
            <td style="border: 1px solid black;">{{ subreddit }}</td>
        </tr>
        {% endfor %}
        </table>
        <br>
        <b>Redditractor by Masa Mark Fukui</b><br>
        https://github.com/Masamerc/redditract-pipeline
        

        '''
    )

    # get_top100_subreddit_names_task >> get_subreddit_data_task >> load_subreddit_to_mongo_task
    get_top100_subreddit_names_task >> get_subreddit_data_task >> load_subreddit_to_mongo_task >> [store_top_submission_to_csv_task, store_hot_submission_to_csv_task, store_new_submission_to_csv_task]
    store_hot_submission_to_csv_task >> load_hot_submission_to_mongo_task >> send_email_task
    store_new_submission_to_csv_task >> load_new_submission_to_mongo_task >> send_email_task
    store_top_submission_to_csv_task >> load_top_submission_to_mongo_task >> send_email_task
