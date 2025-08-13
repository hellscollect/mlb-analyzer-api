# MLB Analyzer API (FastAPI)

## Deploy on Render (no terminal)
1. Create a **Web Service** on Render from this GitHub repo.
2. Settings:
   - **Environment:** Python
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
3. Deploy.

When Render shows the service URL (e.g., `https://YOUR.onrender.com`), open:
- `https://YOUR.onrender.com/health`  → JSON with `"provider_loaded": true`
- `https://YOUR.onrender.com/openapi.json` → OpenAPI JSON

> Optional: If you want to use a different provider than the default stub, set an environment variable in Render:
> Name: `MLB_PROVIDER`  
> Value: `providers.your_file:YourClass`

## Connect your GPT (ChatGPT Actions)
1. ChatGPT → **My GPTs** → your GPT → **Edit** → **Configure**.
2. **Actions** → **Add action** → **Import from URL**.
3. Paste: `https://YOUR.onrender.com/openapi.json`
4. Save the action and Save the GPT.
5. In a chat with your GPT, run:
   - `Call /health`
   - `Call /provider_raw?date=today&debug=1`
   - `Call /slate_scan?date=today&debug=1`

These will return 200 with valid (empty) data from the stub provider.
