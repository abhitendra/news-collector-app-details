from celery import Celery
from feed_parser import parse_feeds
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Create a Celery application
app = Celery('tasks', broker='pyamqp://guest@localhost//')

# Database connection URL
DATABASE_URL = 'postgresql://user:password@localhost/mydatabase'

def get_db_session(database_url):
    # Create a new SQLAlchemy engine
    engine = create_engine(database_url)  # This will use psycopg2-binary under the hood
    Session = sessionmaker(bind=engine)
    return Session()

@app.task
def process_feeds():
    # Get a new session for database operations
    session = get_db_session(DATABASE_URL)
    # Call the function to parse feeds with the database session
    parse_feeds(session)
