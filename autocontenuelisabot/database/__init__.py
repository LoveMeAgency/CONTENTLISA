from sqlalchemy import create_engine, Column, Integer, TEXT
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


engine = create_engine('sqlite:///database/db.sqlite3')


Session = sessionmaker(bind=engine)


Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer,primary_key=True)
    user_id = Column(Integer)
    first_name = Column(TEXT)
    username = Column(TEXT,nullable=True)


    def __init__(self,user_id,first_name,username):
        self.user_id = user_id
        self.first_name = first_name
        self.username = username

        


    
    @classmethod
    def add_user_to_db(cls,user_id,first_name,username):
        with Session() as session:
            user = session.query(cls).filter(cls.user_id == user_id).first()
            if not user:
                session.add(cls(user_id,first_name,username,))
                session.commit()

    @classmethod
    def get_user(cls):
        with Session() as session:
            return session.query(cls).all()


Base.metadata.create_all(engine)
session = Session()
