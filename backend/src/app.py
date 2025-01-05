import os
import time
from dotenv import load_dotenv
from flask import Flask
from controllers.indent_so_controller import file_blueprint
from asgiref.wsgi import WsgiToAsgi
from flask_cors import CORS
import uvicorn

# Track startup time
start_time = time.time()

# Load environment variables from .env file
load_dotenv()

def create_app():
    app = Flask(__name__)
    print(f"Flask initialization took {time.time() - start_time} seconds")

    # Enable CORS (adjust for production)
    CORS(app, resources={r"/*": {"origins": "*"}})

    # Set up the upload folder path
    app.config['UPLOAD_FOLDER'] = os.path.join(os.getcwd(), 'tmp')

    # Register Blueprints (modularized routes for file handling)
    app.register_blueprint(file_blueprint)
    print(f"Blueprint registration took {time.time() - start_time} seconds")

    # Convert the Flask (WSGI) app to ASGI app using WsgiToAsgi for async handling
    return WsgiToAsgi(app)

asgi_app = create_app()

if __name__ == "__main__":
    # Check environment
    env = os.getenv('FLASK_ENV', 'development')
    print(f"Running in {env} mode...")

    if env == 'development':
        # In development, run with uvicorn
        uvicorn.run("app:asgi_app", host="0.0.0.0", port=5000)
    else:
        # In production, use Gunicorn (for example)
        print("Running in production mode...")
        pass
