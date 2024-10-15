# schemas.py
from pydantic import BaseModel
from typing import List, Optional

# Base schema for items
class ItemBase(BaseModel):
    title: str
    description: Optional[str] = None

# Schema for creating an item
class ItemCreate(ItemBase):
    pass

# Full schema representation of an item, including the owner ID
class Item(ItemBase):
    id: int
    owner_id: int  # This links the item to its owner

    class Config:
        orm_mode = True  # Enables compatibility with ORM models

# Base schema for users
class UserBase(BaseModel):
    name: str
    email: str

# Schema for creating a user
class UserCreate(UserBase):
    password: str  # Add password for user creation

# Full schema representation of a user, including related items
class User(UserBase):
    id: int
    items: List[Item] = []  # List of items owned by the user

    class Config:
        orm_mode = True  # Enables compatibility with ORM models

