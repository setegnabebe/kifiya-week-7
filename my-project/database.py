# database.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Database URL (adjust this to match your actual database settings)
DATABASE_URL = "postgresql://postgres:new_password@localhost:5432/tlgdata"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependency to get a DB session in a request
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
