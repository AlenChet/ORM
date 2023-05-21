from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from models_dz import Publisher, Book, Sale, Shop, Stock
import json
from config import db_username, db_password, db_name
from sqlalchemy import select
from models_dz import Base
from datetime import datetime

URL = f"postgresql://{db_username}:{db_password}@localhost/{db_name}"
engine = create_engine(URL)

Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session = Session()

with open('tests_data.json') as f:
    data = json.load(f)

connection = engine.connect()

def get_shops(publisher_indetifer):
    book = Book.__table__
    sale = Sale.__table__
    stock = Stock.__table__
    shop = Shop.__table__
    publisher = Publisher.__table__

    query = (
        select([book.c.title, shop.c.name, sale.c.price, sale.c.date_sale])
        .select_from(
            shop.join(stock, shop.c.id == stock.c.id_shop)
            .join(book, stock.c.id_book == book.c.id)
            .join(publisher, book.c.id_publisher == publisher.c.id)
            .join(sale, stock.c.id == sale.c.id_stock)
        )
    )
    if publisher_indetifer.isdigit():
        query = query.where(publisher.c.id == int(publisher_indetifer))
    else:
        query = query.where(publisher.c.name == publisher_indetifer)

    results = session.execute(query).fetchall()

    if not results:
        print("Такого публициста в списке нет")
    else:
        for title, shop_name, price, date_sale in results:
            formatted_date = date_sale.strftime('%d-%m-%Y')
            print(f"Название книги: {title} | Магазин: {shop_name} | Стоимость продажи: "
                  f"{price} | Дата покупки: {formatted_date}")

if __name__ == '__main__':
    publisher_identifier = input("Введите имя или идентификатор публициста: ")
    get_shops(publisher_identifier)

with open('tests_data.json') as f:
    data = json.load(f)

for record in data:
    model_class = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    model = model_class(id=record.get('pk'), **record.get('fields'))
    session.add(model)

# session.commit()
session.close()


