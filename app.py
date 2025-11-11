from flask import Flask, redirect, url_for
from models import db
from routes.driver import driver_bp
from routes.vehicle import vehicle_bp
from pyngrok import ngrok

app = Flask(__name__)
app.config.from_object('config.Config')
db.init_app(app)

# Register blueprints
app.register_blueprint(driver_bp, url_prefix='/driver')
app.register_blueprint(vehicle_bp, url_prefix='/vehicle')

@app.route('/')
def index():
    return redirect(url_for('vehicle.vehicle_dashboard'))

if __name__ == '__main__':
    # ⚡ Set your ngrok auth token here
    ngrok.set_auth_token("2zJn2rtgFDmbz9BlcmgUwkz5Tg3_JZ5wkJB6eDTE25cvh6NP")

    # Open an ngrok tunnel to the app port
    public_url = ngrok.connect(8080)  # Make sure this matches app.run port
    print(f'Public URL: {public_url}')

    # Run Flask
    app.run(port=8080, debug=True)
