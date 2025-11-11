from flask import Flask, redirect, url_for
from models import db
from routes.driver import driver_bp
from routes.vehicle import vehicle_bp


app = Flask(__name__)
app.config.from_object('config.Config')

db.init_app(app)

# Register blueprints
app.register_blueprint(driver_bp, url_prefix='/driver')
app.register_blueprint(vehicle_bp, url_prefix='/vehicle')

@app.route('/')
def index():
    return redirect(url_for('vehicle.vehicle_dashboard'))  # Default landing page

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

