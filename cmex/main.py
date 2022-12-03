# main.py
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.params import Depends
from sqlalchemy.orm import Session

from . import models, crud, schemas
from .database import engine, SessionLocal

# Auto creation of database tables
#   If tables already exist, this command does nothing. This allows to
#   safely execute this command at any restart of the application.
#   For a better management of the database schema, it is recommended to
#   integrate specific tools, such as Alembic
models.Base.metadata.create_all(bind=engine)

# Application bootstrap
#app = FastAPI()
app = FastAPI(
    title='Panamericano API Documentation', docs_url="/api", openapi_url="/api/v1"
)

# This function represents a dependency that can be injected in the endpoints of the API.
# Dependency injection is very smart, as it allows to declaratively require some service.
# This function models the database connection as a service, so that it can be required
# just when needed.


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# LIST
# This endpoint returns a list of objects of type `Item` serialized using the `Item` schema that
# we defined in schemas.py. The objects exposed are instances of `models.Item` that are
# validated and serialized as of the definition of the schema `schemas.Item`
@app.get("/items", response_model=List[schemas.Item])
def items_action_list(limit: int = 100, offset: int = 0, db: Session = Depends(get_db)):
    items = crud.list_items(db, offset, limit)
    return items

# RETRIEVE
# This endpoint returns a specific `Item`, given the value of its `id` field,
# which is passed as a path parameter in the URL. It can also return some
# error condition in case the identifier does not correspond to any object


@app.get("/items/{item_id}", response_model=schemas.Item)
def items_action_retrieve(item_id: int, db: Session = Depends(get_db)):
    item = crud.get_item(db, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Book not found")
    return item

# CREATE
# This endpoint creates a new `Item`. The necessary data is read from the request
# body, which is parsed and validated according to the ItemCreate schema defined beforehand


@app.post("/items", response_model=schemas.ItemCreate)
def item_action_create(data: schemas.ItemCreate, db: Session = Depends(get_db)):
    item = crud.create_item(db, data)
    return item

# UPDATE
# This endpoint allows to update an existing `Item`, identified by its primary key passed as a
# path parameter in the url. The necessary data is read from the request
# body, which is parsed and validated according to the ItemUpdate schema defined beforehand


@app.put("/items/{item_id}", response_model=schemas.Item)
def items_action_retrieve(item_id: int, data: schemas.ItemUpdate,  db: Session = Depends(get_db)):
    item = crud.update_item(db, item_id, data)
    if item is None:
        raise HTTPException(status_code=404)
    return item


# DELETE
# This endpoint allows to delete an `Item`, identified by its primary key passed as a
# path parameter in the url. It's worth observing that the status code of the response is
# HTTP 204 No Content, since the response body is empty
@app.delete("/items/{item_id}", status_code=204)
def items_action_retrieve(item_id: int,  db: Session = Depends(get_db)):
    crud.drop_item(db, item_id)
    return None
