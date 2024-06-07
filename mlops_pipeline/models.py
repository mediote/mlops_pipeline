from sqlalchemy import (TIMESTAMP, Column, Double, Integer, String,
                        create_engine)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Experiment(Base):
    __tablename__ = 'experiments'
    id_experimento = Column(Integer, primary_key=True)
    nome_modal = Column(String)
    nome_projeto = Column(String)
    # Outros campos...

def get_engine():
    return create_engine('sqlite:///mlops_pipeline.db')

def create_tables():
    engine = get_engine()
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    create_tables()
    create_tables()
