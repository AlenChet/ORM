from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Publisher(Base):
    __tablename__ = "publisher"

    id = Column(Integer,primary_key=True)
    name = Column(String)

    books = relationship('Book', back_populates='publisher')

class Book(Base):
    __tablename__ = 'book'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    id_publisher = Column(Integer, ForeignKey('publisher.id'))
    publisher = relationship('Publisher',  back_populates='books' )
    sales = relationship('Sale', back_populates='book')
    stocks = relationship('Stock', back_populates='book')

class Stock(Base):
    __tablename__ = 'stock'

    id = Column(Integer, primary_key=True)
    id_book = Column(Integer, ForeignKey('book.id'))
    id_shop = Column(Integer, ForeignKey('shop.id'))
    count = Column(Integer)
    book = relationship('Book', back_populates='stocks')
    shop = relationship('Shop', back_populates='stocks')
    sales = relationship('Sale', back_populates='stock')

class Shop(Base):
    __tablename__ = 'shop'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    stocks = relationship('Stock', back_populates='shop')

class Sale(Base):
    __tablename__ = 'sales'

    id = Column(Integer, primary_key=True)
    price = Column(String)
    date_sale = Column(DateTime)
    id_stock = Column(Integer, ForeignKey('stock.id'))
    id_book = Column(Integer, ForeignKey('book.id'))
    count = Column(Integer)
    book = relationship('Book', back_populates='sales')
    stock = relationship('Stock', back_populates='sales')