from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
#from airflow.operators.postgres_operator import PostgresOperator

from datetime import datetime
import pymongo as MongoClient


def scrapeReviews():
        import time
        import pandas as pd
        from dags.scraper.AmazonScraper import AmazonScraper
        reviews = []
        amz_scraper = AmazonScraper()
        product_url = 'https://www.amazon.com/LG-77-Inch-Refresh-AI-Powered-OLED77C2PUA/product-reviews/B09RMSPSK1/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
        for page_num in range(3):
            reviews.extend(amz_scraper.scrapeReviews(url=product_url, page_num=page_num))
            time.sleep(1)
        print('Reviews scraped')
        df = pd.DataFrame(reviews)
        print('Dataframe created')     
        df.to_excel('dags/excelData/reviews.xlsx', index=False)
        print('Dataframe saved to excel')

def sendToDB():
    #import pymongo as MongoClient
    import pandas as pd
    from dags.scraper.SendToDB import SendToDB
    SendToDB.SendToMongo(df ='df', collectionName="amazonReviews", mongoDBConnection='mongodb+srv://garytwotimes:LRcstOD6ZUblNJ26@cluster0.i7c5ze6.mongodb.net/test')
    #SendToDB.checkForDuplicates(df='df')
    print('Data sent to mongo atlas')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
}
with DAG(dag_id='scraperDAG', default_args=default_args, start_date=datetime(2020, 1, 1),
    schedule='@hourly', description="Scraping e-commerce reviews",
     tags=["web scraping", "Garrett"], catchup=False) as dag:
     #set up a DAG that triggers demo.py to scrape reviews and then send the data to postgress and mongodb
        # python operator to trigger scrapeReviews task from scraperDag
    scrapeReviews = PythonOperator(
        task_id='scrapeReviews',
        python_callable=scrapeReviews,
        dag=dag
    )
        # python operator to trigger SendToDBs from SendToDB.py

    sendToDB = PythonOperator(
        task_id='sendToDB',
        python_callable=sendToDB,
        dag=dag
    )
   



#set up dependencies
scrapeReviews >> sendToDB 







        
    
    
   


    






  
  



    

    



