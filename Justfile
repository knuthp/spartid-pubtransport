



serve: serve_api
    @echo "Serving application on http://localhost:3000"

serve_api:
    @echo "Serving API on http://localhost:8000"
    uv run uvicorn spartid_pubtransport.api.api:app --reload --host 0.0.0.0 --port 8000