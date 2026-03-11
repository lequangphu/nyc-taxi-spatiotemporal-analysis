# Streamlit Cloud Deployment Guide

## Quick Start

### 1. Update Streamlit Cloud Configuration

**IMPORTANT**: After the package restructure, you must update the main file path.

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Click on your app: `nyc-taxi-spatiotemporal-analysis`
3. Go to **Settings** → **Advanced**
4. Change **Main file path** from `src/dashboard/app.py` to `streamlit_app.py`
5. Click **Save changes**

### 2. Push Changes to GitHub

The new structure requires these files to be pushed:

```bash
git add .
git commit -m "Fix package structure for Streamlit Cloud deployment"
git push
```

### 3. Deployment Details

Streamlit Cloud will automatically:
- Detect `uv.lock` and use `uv-sync` to install dependencies
- Alternatively fall back to `requirements.txt` if needed
- Run the app using `streamlit_app.py` as the entry point

## File Structure for Deployment

```
nyc-taxi-spatiotemporal-analysis/
├── streamlit_app.py          # Main entry point (update Streamlit Cloud to use this)
├── pyproject.toml            # Package configuration
├── uv.lock                   # Dependency lock file (auto-generated)
├── requirements.txt          # Fallback dependencies
├── packages.txt              # System dependencies (empty for this app)
├── .streamlit/
│   └── config.toml           # Streamlit configuration
└── src/
    └── nyc_taxi_spatiotemporal_analysis/
        ├── __init__.py
        ├── data/
        ├── eda/
        ├── zones/
        ├── anomaly/
        └── dashboard/
            └── app.py        # Main dashboard code
```

## Configuration Files

### pyproject.toml
```toml
[tool.hatch.build.targets.wheel]
packages = ["src/nyc_taxi_spatiotemporal_analysis"]
```

### .streamlit/config.toml
```toml
[theme]
primaryColor = "#F63366"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
textColor = "#262730"
font = "sans serif"

[client]
showErrorDetails = false

[server]
headless = true
```

## Troubleshooting

### "ModuleNotFoundError: No module named 'src'"

This error should be fixed with the new structure. If it persists:

1. Verify the main file path is `streamlit_app.py` (not `src/dashboard/app.py`)
2. Ensure all changes are pushed to GitHub
3. Check that the package is properly installed by viewing the deployment logs

### Import Errors

If you see import errors:

1. Check that `pyproject.toml` has the correct `packages` setting
2. Verify `streamlit_app.py` exists in the repo root
3. Ensure the repository has the latest changes from `main` branch

### Data Directory Issues

The app uses `Path("data")` for the data directory. Streamlit Cloud will create this directory automatically when the app downloads data.

## Local Testing

Test locally before deploying:

```bash
# Using uv (recommended)
uv sync
uv run streamlit run streamlit_app.py

# Or using pip
pip install -e .
streamlit run streamlit_app.py
```

## Monitoring

After deployment:
- Check the app logs at: Streamlit Cloud → Your app → **Logs**
- Monitor for any import errors or startup issues
- The first run may take longer as data is downloaded

## Deployment URL

Your app should be available at:
```
https://nyc-taxi-spatiotemporal-analysis-vqgaxmmuiddjvq4rnpjnll.streamlit.app/
```

## Support

For issues:
1. Check the [Streamlit Cloud documentation](https://docs.streamlit.io/streamlit-cloud/get-started)
2. Review deployment logs in Streamlit Cloud dashboard
3. Verify the package structure matches this guide
