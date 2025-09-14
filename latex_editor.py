from flask import Blueprint, render_template

latex_bp = Blueprint('latex_editor', __name__, url_prefix='/admin')

@latex_bp.route('/latex_editor')
def latex_editor():
    return render_template('latex_editor.html')
