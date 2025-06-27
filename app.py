from pydantic import BaseModel
import psycopg2

app = FastAPI()

# ---------- Database Connection ----------
conn = psycopg2.connect(
    dbname="demo",
    user="postgres",
    password="root",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# ---------- Create Table if Not Exists ----------
cur.execute("""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255) UNIQUE NOT NULL,
        author VARCHAR(255) NOT NULL,
        category VARCHAR(255) NOT NULL
    )
""")
conn.commit()

# ---------- Input Schema ----------
class BookSchema(BaseModel):
    title: str
    author: str
    category: str

# ---------- 1. Get All Books ----------
@app.get("/books")
def get_all_books():
    cur.execute("SELECT title, author, category FROM books")
    print(cur)
    rows = cur.fetchall()
    books = []
    for row in rows:
        books.append({
            "title": row[0],
            "author": row[1],
            "category": row[2]
        })
    return books

# # ---------- 2. Get Book by Title ----------
@app.get("/books/{title}")
def get_book_by_title(title: str):
    cur.execute(f"SELECT title, author, category FROM books WHERE LOWER(title) = LOWER(%s)", (title,))
    book = cur.fetchone()
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return {
        "title": book[0],
        "author": book[1],
        "category": book[2]
    }

# ---------- 3. Create Book ----------
@app.post("/books/create")
def create_book(book: BookSchema):
    cur.execute("SELECT title FROM books WHERE LOWER(title) = LOWER(%s)", (book.title,))
    if cur.fetchone():
        raise HTTPException(status_code=400, detail="Book already exists")
    cur.execute(
        "INSERT INTO books (title, author, category) VALUES (%s, %s, %s)",
        (book.title, book.author, book.category)
    )
    conn.commit()
    return {
        "message": "Book created successfully",
        "data": book.dict()
    }

# ---------- 4. Update Book ----------
@app.put("/books/update")
def update_book(book: BookSchema):
    cur.execute("SELECT title FROM books WHERE LOWER(title) = LOWER(%s)", (book.title,))
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Book not found")
    cur.execute(
        "UPDATE books SET author = %s, category = %s WHERE LOWER(title) = LOWER(%s)",
        (book.author, book.category, book.title)
    )
    conn.commit()
    return {
        "message": "Book updated successfully",
        "data": book.dict()
    }

# ---------- 5. Delete Book ----------
@app.delete("/books/delete/{title}")
def delete_book(title: str):
    cur.execute("SELECT title FROM books WHERE LOWER(title) = LOWER(%s)", (title,))
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Book not found")
    cur.execute("DELETE FROM books WHERE LOWER(title) = LOWER(%s)", (title,))
    conn.commit()
    return {"message": f"Book '{title}' deleted successfully"}
