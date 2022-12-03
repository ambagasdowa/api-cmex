# schemas.py
from typing import Optional, Union
from datetime import date, datetime, timedelta

from pydantic import BaseModel

#  `id`                      int unsigned not null auto_increment primary key, -- --> Cual es el pedo?
#  `book_id`                 varchar(255) not null , -- --> ex: 228
#  `pages`                   int null, -- --> 8 total pages
#  `book_name`               varchar(255) null, -- --> Guia_UV
#  `is_url`          bool not null default false, -- --> means false is path url/{book_id} else url?book_id={id}&var=foo
#  `created`                 datetime,
#  `modified`                datetime,
#  `status`                  bool not null default true


class ItemBase(BaseModel):
    book_id: str
    pages: int
    book_name: str
    is_url: bool
    created: datetime
    modified: datetime
    status: bool


class ItemCreate(BaseModel):
    book_id: str
    pages: int
    book_name: str
    is_url: bool
    created: datetime
    status: bool


class ItemUpdate(BaseModel):
    book_id: str
    pages: int
    book_name: str
    is_url: bool
    modified: datetime
    status: bool


class Item(ItemBase):
    id: int

    class Config:
        orm_mode = True
