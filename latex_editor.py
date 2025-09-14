from flask import Blueprint, render_template

latex_bp = Blueprint('latex_editor', __name__, url_prefix='/admin')

@latex_bp.route('/latex_editor')
def latex_editor():
    
    from admin import admin_required
    @admin_required
    def _inner():
        return render_template('latex_editor.html')

    return _inner()
