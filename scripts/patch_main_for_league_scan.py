# scripts/patch_main_for_league_scan.py
import re, sys, time, os

MAIN_PATH = os.environ.get("MAIN_FILE", "main.py")
IMPORT_LINE = "from routes.league_scan import router as league_scan_router"
INCLUDE_LINE = "app.include_router(league_scan_router)"

def read(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def backup(path):
    ts = time.strftime("%Y%m%d-%H%M%S")
    bak = f"{path}.bak.{ts}"
    with open(path, "rb") as src, open(bak, "wb") as dst:
        dst.write(src.read())
    print(f"[ok] Backup written: {bak}")

def insert_import(content):
    if IMPORT_LINE in content:
        return content, False

    # Find the end of the top import block (from/import lines at file start)
    lines = content.splitlines()
    idx = 0
    started = False
    last_import_idx = -1
    for i, line in enumerate(lines):
        if not started and (line.strip().startswith("import ") or line.strip().startswith("from ")):
            started = True
            last_import_idx = i
        elif started:
            if line.strip().startswith("import ") or line.strip().startswith("from "):
                last_import_idx = i
            else:
                break

    insert_at = last_import_idx + 1 if last_import_idx >= 0 else 0
    lines.insert(insert_at, IMPORT_LINE)
    return "\n".join(lines) + ("\n" if not content.endswith("\n") else ""), True

def insert_include(content):
    if INCLUDE_LINE in content:
        return content, False

    lines = content.splitlines()
    # Prefer to add after the last existing include_router(...)
    last_include_idx = -1
    for i, line in enumerate(lines):
        if "app.include_router(" in line:
            last_include_idx = i

    if last_include_idx >= 0:
        insert_at = last_include_idx + 1
        lines.insert(insert_at, INCLUDE_LINE)
        return "\n".join(lines) + ("\n" if not content.endswith("\n") else ""), True

    # Otherwise, add right after app = FastAPI(...)
    app_create_idx = -1
    for i, line in enumerate(lines):
        if re.search(r"\bapp\s*=\s*FastAPI\s*\(", line):
            app_create_idx = i
            break
    if app_create_idx >= 0:
        insert_at = app_create_idx + 1
        lines.insert(insert_at, INCLUDE_LINE)
        return "\n".join(lines) + ("\n" if not content.endswith("\n") else ""), True

    # If we can't find either, append at the end (last resort)
    lines.append(INCLUDE_LINE)
    return "\n".join(lines) + ("\n" if not content.endswith("\n") else ""), True

def main():
    if not os.path.exists(MAIN_PATH):
        print(f"[err] {MAIN_PATH} not found. Set MAIN_FILE env var if your file has a different name.")
        sys.exit(1)

    content = read(MAIN_PATH)
    backup(MAIN_PATH)

    content, imp_added = insert_import(content)
    content, incl_added = insert_include(content)

    if imp_added or incl_added:
        write(MAIN_PATH, content)
        print(f"[ok] Patched {MAIN_PATH}: import_added={imp_added}, include_added={incl_added}")
    else:
        print(f"[ok] No changes needed. {MAIN_PATH} already has the import and include.")

if __name__ == "__main__":
    main()
