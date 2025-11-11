import os

class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "your-secret-key")
    SQLALCHEMY_DATABASE_URI = "postgresql://edge_admin:OlU01)vX0B!@173.249.46.82:5432/postgres"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

