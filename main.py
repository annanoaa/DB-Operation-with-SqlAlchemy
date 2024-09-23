from sqlalchemy import create_engine, Column, Integer, String, Table, ForeignKey, Date, func
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm import declarative_base
from faker import Faker
import random

# Create a Faker instance
fake = Faker()

# Define the base class
Base = declarative_base()

# Association table for the many-to-many relationship between Author and Book
author_book = Table('author_book', Base.metadata,
                    Column('author_id', Integer, ForeignKey('author.id'), primary_key=True),
                    Column('book_id', Integer, ForeignKey('book.id'), primary_key=True))

# Define the Author table
class Author(Base):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True, autoincrement=True)
    firstname = Column(String, nullable=False)
    lastname = Column(String, nullable=False)
    birthdate = Column(Date, nullable=False)
    birthplace = Column(String, nullable=False)

    # Relationship to books (many-to-many)
    books = relationship('Book', secondary=author_book, back_populates='authors')

# Define the Book table
class Book(Base):
    __tablename__ = 'book'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    category = Column(String, nullable=False)
    number_of_pages = Column(Integer, nullable=False)
    release_date = Column(Date, nullable=False)

    # Relationship to authors (many-to-many)
    authors = relationship('Author', secondary=author_book, back_populates='books')

# Set up the SQLite database
engine = create_engine('sqlite:///library.db')

# Create the tables
Base.metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Function to insert authors
def insert_authors(num_authors):
    for _ in range(num_authors):
        firstname = fake.first_name()
        lastname = fake.last_name()
        birthdate = fake.date_of_birth()
        birthplace = fake.city()

        author = Author(firstname=firstname, lastname=lastname, birthdate=birthdate, birthplace=birthplace)
        session.add(author)
    session.commit()

# Function to insert books
def insert_books(num_books):
    authors = session.query(Author).all()

    for _ in range(num_books):
        title = fake.sentence(nb_words=4)
        category = random.choice(['Fiction', 'Non-Fiction', 'Sci-Fi', 'Fantasy', 'Mystery', 'Romance', 'History'])
        number_of_pages = random.randint(100, 1200)
        release_date = fake.date_between(start_date='-150y', end_date='now')

        book = Book(title=title, category=category, number_of_pages=number_of_pages, release_date=release_date)

        # Assign random authors to this book (many-to-many)
        book.authors = random.sample(authors, random.randint(1, 3))  # Each book has 1-3 authors
        session.add(book)
    session.commit()

# Insert 500 authors and 1000 books into the database
insert_authors(500)
insert_books(1000)

print("Database populated successfully.")

# Function to query books with maximum pages
def books_with_max_pages():
    max_pages = session.query(func.max(Book.number_of_pages)).scalar()
    books = session.query(Book).filter(Book.number_of_pages == max_pages).all()

    print("\nBooks with max pages:")
    for book in books:
        print(book.title, book.number_of_pages)

# Function to calculate average number of pages
def avg_number_of_pages():
    avg_pages = session.query(func.avg(Book.number_of_pages)).scalar()
    print(f"\nThe average number of pages is {avg_pages:.2f}")

# Function to find the youngest author
def youngest_author():
    youngest_birthdate = session.query(func.max(Author.birthdate)).scalar()
    authors = session.query(Author).filter(Author.birthdate == youngest_birthdate).all()

    print("\nThe youngest author(s):")
    for author in authors:
        print(f"{author.firstname} {author.lastname}, born {author.birthdate}")

# Function to find authors without books
def author_without_book():
    authors = session.query(Author).outerjoin(Author.books).filter(Book.id == None).all()

    print("\nAuthors without books:")
    for author in authors:
        print(f"{author.firstname} {author.lastname}")

# Function to find 5 authors with more than 3 books
def author_with_morethan_3books():
    authors = session.query(Author).join(Author.books).group_by(Author.id).having(func.count(Book.id) > 3).limit(5).all()

    print("\nAuthors with more than 3 books:")
    for author in authors:
        print(f"{author.firstname} {author.lastname}")

# Execute the queries
books_with_max_pages()
avg_number_of_pages()
youngest_author()
author_without_book()
author_with_morethan_3books()

# Close the session
session.close()