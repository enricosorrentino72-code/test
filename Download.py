import os
import requests

def download_model(url, output_path, min_size_mb=1):
    """Download a model file from a URL with size validation."""
    try:
        print(f"📥 Downloading from: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        # Verifica dimensione
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        if size_mb < min_size_mb:
            print(f"⚠️ Warning: File too small ({size_mb:.2f} MB). Possibly an error page was downloaded.")
            return False
        print(f"✅ Download complete: {output_path} ({size_mb:.2f} MB)")
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ Download failed: {e}")
        return False

# Esegui
url = "https://github.com/smaranjitghose/PlantDoc/releases/download/v1.0/yolov8n-plantdoc.pt"
output_path = "yolov8n-plantdoc.pt"

if download_model(url, output_path):
    print("🎯 Ready to use the model.")
else:
    print("🚫 Model download failed or incomplete.")
