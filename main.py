from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from models_dz import Publisher, Book, Sale, Shop, Stock
import json
from config import db_username, db_password, db_name
from sqlalchemy import select
from models_dz import Base

URL = f"postgresql://{db_username}:{db_password}@localhost/{db_name}"
engine = create_engine(URL)

Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session = Session()

with open('tests_data.json') as f:
    data = json.load(f)

connection = engine.connect()

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

session.commit()


def find_shop(publisher_name):
    book = Book.__table__
    sale = Sale.__table__
    stock = Stock.__table__
    shop = Shop.__table__
    publisher = Publisher.__table__

    query = (
        select([book.c.title, shop.c.name, sale.c.price, sale.c.date_sale])
        .select_from(
            book.join(stock, book.c.id == stock.c.id_book)
            .join(shop, stock.c.id_shop == shop.c.id)
            .join(sale, stock.c.id == sale.c.id_stock)
            .join(publisher, book.c.id_publisher == publisher.c.id)
        )
        .where(publisher.c.name == publisher_name)
    )

    sales = connection.execute(query).fetchall()

    connection.close()

    for sale in sales:
        title, shop_name, price, date_sale = sale
        print(f"Название книги: {title} | Магазин: {shop_name} | Стоимость покупки: "
              f"{price} | Дата покупки: {date_sale}")


publisher_name = input("Введите имя или идентификатор издателя: ")

print(find_shop(publisher_name))

session.close()

