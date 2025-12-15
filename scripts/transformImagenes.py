from PIL import Image
from pathlib import Path

INPUT_DIR = Path("../data/imagenes")
OUTPUT_DIR = Path("../output/imagenes_procesadas")

OUTPUT_DIR.mkdir(exist_ok=True)

for img_path in INPUT_DIR.iterdir():
    if img_path.suffix.lower() not in [".png", ".jpg", ".jpeg"]:
        continue

    with Image.open(img_path) as img:
        img = img.convert("RGB")
        img = img.resize((800, 600))

        output_file = OUTPUT_DIR / f"{img_path.stem}.jpg"
        img.save(output_file, "JPEG")

print("✅ Imágenes transformadas correctamente")