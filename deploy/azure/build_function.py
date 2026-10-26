"""Build the Function deployment zip: function_app/* + the fulfillment_risk.signals package (nothing else).
usage: python deploy/azure/build_function.py [out.zip]  -> prints the zip path"""
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(tempfile.gettempdir()) / "fr-signals-func.zip"
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
    for f in ("function_app.py", "host.json", "requirements.txt"):
        z.write(ROOT / "function_app" / f, f)
    z.writestr("fulfillment_risk/__init__.py", (ROOT / "fulfillment_risk" / "__init__.py").read_text())
    for f in sorted((ROOT / "fulfillment_risk" / "signals").glob("*")):
        if f.suffix in (".py", ".sql") and f.name != "__main__.py":
            z.write(f, f"fulfillment_risk/signals/{f.name}")
print(out)
