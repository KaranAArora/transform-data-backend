from flask import Blueprint, request, send_file, jsonify, current_app, after_this_request
from services.indent_to_so import indent_so_process
from werkzeug.utils import secure_filename
import os
import asyncio
from utils.dely_delete import delayed_file_deletion

# Initialize Blueprint for handling file-related routes
file_blueprint = Blueprint('file', __name__)

ALLOWED_EXTENSIONS = {'csv'}

# Helper function to check allowed file extensions
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Route to handle file upload and processing
@file_blueprint.route('/v1/api/indent-to-so', methods=['POST'])
async def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    print(request.files)
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    if not allowed_file(file.filename):
        return jsonify({"error": "Invalid file type"}), 400
    
    filename = secure_filename(file.filename)
    filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
    #print(f'In Controller:{filepath}')
    file.save(filepath)

    # Process file using service
    try:

        processed_file_path, processed_file_name = await indent_so_process(filepath)
        print(processed_file_path)
        
        # Send the saved file as a downloadable attachment
        response = send_file(
            processed_file_path,
            as_attachment=True,
            download_name=processed_file_name,
            mimetype="text/csv"
        )
        print(response)
        delayed_file_deletion(processed_file_path, delay=15)
        return response

    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

 
        
