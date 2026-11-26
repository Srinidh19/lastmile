# LastMile AI — Deployment Guide

This guide details how to deploy the complete **LastMile AI** platform:
- **Frontend Dashboard**: [Streamlit Community Cloud](https://share.streamlit.io)
- **Backend API & Simulator Worker**: [Render.com](https://render.com) (or [Railway.app](https://railway.app))

---

## Architecture Overview

```
┌─────────────────────────────────┐      HTTPS       ┌─────────────────────────────────┐
│   Streamlit Community Cloud     │ ───────────────> │           Render.com            │
│   (frontend/app.py)             │                  │  • Web Service (FastAPI :8000)  │
│   https://<your-app>.streamlit.app                 │  • Worker (vehicle_simulator)   │
└─────────────────────────────────┘                  └─────────────────────────────────┘
```

---

## Step 1: Push Code to GitHub

Streamlit Community Cloud and Render deploy directly from a GitHub repository.

### Option A: Via GitHub Web (Easiest if Git CLI is not installed)
1. Go to [github.com/new](https://github.com/new) and create a repository named `LastMile-AI` (Public).
2. Click **"uploading an existing file"**.
3. Drag and drop all project folders (`backend`, `frontend`, `services`, `simulator`, `requirements.txt`, `render.yaml`, `Procfile`, `.streamlit`) and click **Commit changes**.

### Option B: Via Git CLI
```bash
git init
git add .
git commit -m "feat: complete LastMile AI with Kafka streaming and cloud deployment"
git branch -M main
git remote add origin https://github.com/<YOUR-USERNAME>/LastMile-AI.git
git push -u origin main
```

---

## Step 2: Deploy Backend & Simulator on Render (Free Tier)

Render hosts both the FastAPI REST/Streaming API and the Python vehicle simulator.

1. Go to [dashboard.render.com](https://dashboard.render.com/) and sign in (via GitHub).
2. Click **New +** -> **Blueprint**.
3. Select your `LastMile-AI` repository.
4. Render will automatically read [`render.yaml`](./render.yaml) and configure:
   - **`lastmile-backend`** (Web Service):
     - Build Command: `pip install -r requirements.txt`
     - Start Command: `uvicorn backend.main:app --host 0.0.0.0 --port $PORT`
   - **`lastmile-simulator`** (Background Worker):
     - Build Command: `pip install -r requirements.txt`
     - Start Command: `python simulator/vehicle_simulator.py`
     - Automatically links `BASE_URL` to `lastmile-backend`.
5. Click **Apply**.
6. Once deployed, copy your backend URL (e.g., `https://lastmile-backend-xxxx.onrender.com`).
   - Verify health: `https://lastmile-backend-xxxx.onrender.com/health`
   - Verify docs: `https://lastmile-backend-xxxx.onrender.com/docs`
   - Verify streaming: `https://lastmile-backend-xxxx.onrender.com/streaming/status`

---

## Step 3: Deploy Frontend on Streamlit Community Cloud (Free Tier)

1. Go to [share.streamlit.io](https://share.streamlit.io/) and sign in with GitHub.
2. Click **"Create app"** -> **"Yup, I have an app"**.
3. Fill in:
   - **Repository**: `<YOUR-USERNAME>/LastMile-AI`
   - **Branch**: `main`
   - **Main file path**: `frontend/app.py`
   - **App URL**: `lastmile-ai` (or your chosen custom slug)
4. Click **Advanced settings** -> **Secrets**:
   Add your Render backend URL:
   ```toml
   API_URL = "https://lastmile-backend-xxxx.onrender.com"
   ```
5. Click **Deploy!**

Your live dashboard will be accessible at:
`https://<your-app-name>.streamlit.app`

---

## Step 4 (Optional): Cloud Kafka / Condense Integration

To stream through your managed Condense cluster on Render:
1. In the Render Dashboard, go to **`lastmile-backend`** -> **Environment**.
2. Add the following variables:
   - `KAFKA_BOOTSTRAP_SERVERS`: `cluster.condense.io:9092`
   - `KAFKA_SECURITY_PROTOCOL`: `SASL_SSL`
   - `KAFKA_SASL_MECHANISM`: `PLAIN`
   - `CONDENSE_API_KEY`: `<your_key>`
   - `CONDENSE_API_SECRET`: `<your_secret>`
3. Save and redeploy. The dashboard badge will automatically switch to:
   `● CONDENSE + KAFKA STREAMING`
