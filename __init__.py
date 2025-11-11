from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

db = SQLAlchemy()
login_manager = LoginManager()

def create_app():
    app = Flask(__name__)
    app.config.from_object("config.Config")

    db.init_app(app)
    login_manager.init_app(app)

    # Import blueprints here (inside factory to avoid circular import)
    from routes.driver import driver_bp
    from routes.vehicle import vehicle_bp

    app.register_blueprint(driver_bp, url_prefix="/driver")
    app.register_blueprint(vehicle_bp, url_prefix="/vehicle")

    return app
