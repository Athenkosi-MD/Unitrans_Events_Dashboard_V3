import os

class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "your-secret-key")
    SQLALCHEMY_DATABASE_URI = "postgresql://postgres:Nicolaas24@localhost:5432/Unitrans_Dashboard"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
