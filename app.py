"""Entry point for the recruiter dashboard:  uv run python app.py   (http://localhost:7860)"""
import os

from recruiting.services.dashboard.ui import build_app

if __name__ == "__main__":
    # PROD: put behind auth / SSO (a recruiter UI exposes candidate data), and serve via a real ASGI server.
    build_app().launch(server_name=os.getenv("GRADIO_SERVER_NAME", "127.0.0.1"), server_port=7860)
