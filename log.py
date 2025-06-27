from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
import psycopg2
import bcrypt

app = FastAPI()

# -------------------- Database Connection --------------------
conn = psycopg2.connect(
    dbname="demo",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# -------------------- Create Users Table --------------------
cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) UNIQUE NOT NULL,
        password TEXT NOT NULL,
        name VARCHAR(255),
        address TEXT,
        aadhar VARCHAR(12)
    )
""")
conn.commit()

# -------------------- Pydantic Schemas --------------------
class RegisterSchema(BaseModel):
    email: EmailStr
    password: str
    name: str
    address: str
    aadhar: str

class LoginSchema(BaseModel):
    email: EmailStr
    password: str

# -------------------- Password Utilities --------------------
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

# -------------------- Register API --------------------
@app.post("/register")
def register(user: RegisterSchema):
    cur.execute("SELECT * FROM users WHERE email = %s", (user.email,))
    if cur.fetchone():
        raise HTTPException(status_code=400, detail="Email already registered")

    hashed_pwd = hash_password(user.password)
    cur.execute("""
        INSERT INTO users (email, password, name, address, aadhar)
        VALUES (%s, %s, %s, %s, %s)
    """, (user.email, hashed_pwd, user.name, user.address, user.aadhar))
    conn.commit()
    return {"message": "User registered successfully"}

# -------------------- Login API --------------------
@app.post("/login")
def login(user: LoginSchema):
    cur.execute("SELECT password FROM users WHERE email = %s", (user.email,))
    row = cur.fetchone()
    if not row or not verify_password(user.password, row[0]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return {"message": f"Welcome {user.email}!"}
