"""ASGI entrypoint for running the Flask + Flask-SocketIO app with uvicorn.

We mount the Flask WSGI app (which already includes the Socket.IO WSGI
middleware) into an ASGI server by first building the WSGI application with
`socketio.WSGIApp` and then converting it with `WsgiToAsgi`.

This approach keeps the Socket.IO server running in WSGI mode (the same mode
used by `python app.py`) while still allowing uvicorn to serve the app.
"""
import threading
import importlib
from asgiref.wsgi import WsgiToAsgi

# Import the Flask app, the Flask-SocketIO instance, and the websocket runner
from app import app, socketio, run_websocket

# Build the WSGI app that combines Socket.IO and Flask, then convert to ASGI
# Prefer using the python-socketio module's WSGIApp and the underlying
# server object exposed by Flask-SocketIO. This avoids passing the
# Flask-SocketIO instance itself (which is not a WSGI callable).
socketio_module = importlib.import_module('socketio')

wsgi_app = None
try:
	# socketio.server should be the underlying python-socketio Server
	if hasattr(socketio, 'server') and socketio.server is not None:
		wsgi_app = socketio_module.WSGIApp(socketio.server, app)
	else:
		# Older/newer versions might expose a wsgi_app attribute on the instance
		if hasattr(socketio, 'wsgi_app') and callable(socketio.wsgi_app):
			wsgi_app = socketio.wsgi_app
		else:
			# Fallback: try to get a WSGI app by asking the socketio module
			# to wrap the Flask app's WSGI app. This should be callable.
			try:
				wsgi_app = socketio_module.WSGIApp(getattr(app, 'wsgi_app', app), app)
			except Exception:
				# As a last resort use the Flask app's WSGI app directly
				wsgi_app = getattr(app, 'wsgi_app', app)
except Exception:
	# If anything goes wrong fall back to the Flask WSGI app so the server
	# still responds to HTTP requests (Socket.IO may be degraded).
	wsgi_app = getattr(app, 'wsgi_app', app)

asgi_app = WsgiToAsgi(wsgi_app)

# Start background websocket client (same behavior as running `python app.py`)
ws_thread = threading.Thread(target=run_websocket, daemon=True)
ws_thread.start()

