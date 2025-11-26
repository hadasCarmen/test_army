from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Response,Depends
from pydantic import BaseModel, EmailStr
import sqlite3
from datetime import datetime
import uvicorn
import csv
import io
from sqlmodel import SQLModel,create_engine,Session,Field,select
from contextlib import asynccontextmanager
from pprint import pprint




DB_FILE = "tower_db.sqlite"

DATABASE_URL = f"sqlite:///{DB_FILE}"

engine = create_engine(DATABASE_URL, echo=True)

# ============================================================================
# Dependency: Database Session
# ============================================================================


def get_session():
    """
    Dependency function that provides a database session.
    FastAPI will call this for each request that needs database access.
    The session is automatically closed after the request.
    """
    with Session(engine) as session:
        yield session

# ============================================================================
# Database Initialization
# ============================================================================


def init_db():
    """Create all tables in the database"""
    SQLModel.metadata.create_all(engine)

# ----------------------------
# Models
# ----------------------------


class Soljer(SQLModel,table=True):
    soder_nomber: int=Field(default=None,primary_key=True,)
    first_name : str
    last_name : str
    gender : str
    city : str
    distance : int
    status : str=Field(default='pending',)
    room:int|None=Field(default=None,foreign_key='room.numberRoom')
    home:int|None=Field(default=None,foreign_key='home.homeNumber')


class Room(SQLModel,table=True):
    numberRoom: int=Field(default=None,primary_key=True) 
    avalible: str|None=Field(default=None,)
    home : int =Field(foreign_key='home.homeNumber')

class Home(SQLModel,table=True):
    homeNumber:int=Field(default=None,primary_key=True) 
    homeRooms:int
    numBeds:int

class NumberHomes(SQLModel,table=True):
    NumberHomes:int=Field(default=None,primary_key=True) 

class List_waiting(SQLModel,table=True):
    pass


class List_not_waiting(SQLModel,table=True):
    pass




 
def import_hayal_from_csv(session: Session, csv_content: bytes) -> dict:
    decoded = csv_content.decode('utf-8')
    # print(decoded)
    reader = csv.DictReader(io.StringIO(decoded))
    # imported_count = 0
    for row in reader:
        # print(row)
        # if not row.get("name") or not row.get("age") or not row.get("email"):
        #     continue
        try:
            soljer = Soljer(
                soder_nomber=row["soder_nomber"],
                first_name=row["first_name"],
                last_name=row["last_name"],
                gender=row["gender"],
                city=row["city"],
                distance =row["distance"] 
            )
            session.add(soljer)
            # imported_count += 1
        except Exception:
            session.rollback()
            # print('hi')
            continue
    session.commit()
    return {
        "message": "haylim imported"
    }






@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI.
    Handles startup and shutdown events.
    In this case we initialize the database on startup and close all connections to it on shutdown.
    """
    # Startup: Initialize database
    init_db()
    yield
    # Shutdown: Close database engine
    engine.dispose()



app = FastAPI(
    title="Soljers Management API (SQLModel)",
    version="1.0.0",
    lifespan=lifespan
)



@app.post("/solgers/AssignWithCSV")
def upload_cars_csv(file: UploadFile = File(...), session: Session = Depends(get_session)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be CSV")
    content =  file.file.read()
    result = import_hayal_from_csv(session, content)
    return result

lifespan(app)



@app.put("/solgers/update-rooms")
def order_first_rooms():
    with Session(engine) as session:
        # all_beds_clear=160 
        stmt = select(Soljer).order_by(Soljer.distance.desc()).limit(160)
        latest = session.exec(stmt).all()
        count=0
        for sol in latest:
            sol_room=(count)%8+1
            count+=1
            soljer=session.get(Soljer,sol.soder_nomber)
            try:
            #     soljer = Soljer(
            #         soder_nomber=sol["soder_nomber"],
            #         first_name=sol["first_name"],
            #         last_name=sol["last_name"],
            #         gender=sol["gender"],
            #         city=sol["city"],
            #         distance =sol["distance"] ,
            #         room=sol_room
            #     )
                setattr(soljer,'room', sol_room)
                setattr(soljer,'status', 'not pending')
                if count<80:
                    setattr(soljer,'home', 1)
                else:
                    setattr(soljer,'home', 2)
                session.add(soljer)
                session.commit()
            except Exception:
                session.rollback()
                # print('hi')
                continue
        # pprint(latest)
    return {
        "message": "room update"
    }

list_waiting=[]
list_not_waiting=[]

@app.get("/solgers/status-rooms")
def list_status():

    with Session(engine) as session:
        stmt = select(Soljer).order_by(Soljer.distance).limit(140)
        latest = session.exec(stmt).all()
        for sol in latest:
            list_waiting.append(sol)
        stmt = select(Soljer).order_by(Soljer.distance.desc()).limit(160)
        latest = session.exec(stmt).all()
        for sol in latest:
            list_not_waiting.append(sol)
    result_all_soljers_stasus=f"list waiting is: {list_waiting},                                                        list not waiting is :{list_not_waiting}"
    return result_all_soljers_stasus


@app.get("/solgers/status-home")
def status_home():
    with Session(engine) as session:
        stmt = select(Home)
        latest = session.exec(stmt).all()
        #think how finish

@app.get("/solgers/waitingList")
def list_waiting_order():
    return list_waiting[::-1]

# # def search_soder_nomber():    #i know that there is wrong in name...but all the place are same
# #     stmt = select(Soljer).where(Soljer.soder_nomber == "IL")
# #     latest = session.exec(stmt).all()


# def search_soder_nomber(soder_nomber: int):
#     with Session(engine) as session:
#         return session.get(Soljer, soder_nomber)


@app.get("/solgers/search")
def get_soljer(soder_nomber:int):
    with Session(engine) as session:
        stmt = select(Soljer).where(Soljer.soder_nomber==soder_nomber)
        soljer = session.exec(stmt).first()
        return soljer





# order_first_rooms()




