"""Build the Function deployment zip: function_app/* + the fulfillment_risk.signals package (nothing else).
Every entry gets Unix mode 0644 - entries without permission bits are unreadable to the Linux worker
("PermissionError ... /home/site/wwwroot/fulfillment_risk/__init__.py").
usage: python deploy/azure/build_function.py [out.zip]  -> prints the zip path"""
import sys
import tempfile
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(tempfile.gettempdir()) / "fr-signals-func.zip"
files = [(ROOT / "function_app" / f, f) for f in ("function_app.py", "host.json", "requirements.txt")]
files.append((ROOT / "fulfillment_risk" / "__init__.py", "fulfillment_risk/__init__.py"))
files += [(f, f"fulfillment_risk/signals/{f.name}") for f in sorted((ROOT / "fulfillment_risk" / "signals").glob("*"))
          if f.suffix in (".py", ".sql") and f.name != "__main__.py"]
stamp = time.localtime()[:6]
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
    for src, arc in files:
        info = zipfile.ZipInfo(arc, date_time=stamp)
        info.external_attr = (0o100644 << 16)
        info.compress_type = zipfile.ZIP_DEFLATED
        z.writestr(info, src.read_bytes())
print(out)
