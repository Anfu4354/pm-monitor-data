#!/usr/bin/env python3
"""
update_json.py
Generates/updates:
 - data/current.json
 - data/annual_pm25.json
 - data/annual_temperature.json
 - data/history.json

Designed for ROI point:
 lat = 25.6866, lon = -100.3161 (Monterrey)
"""
import ee
import json
import os
import datetime
import time
from github import Github, Auth

# -------------------------
# INITIALIZE EE (service account keys should be available in the environment when running locally)
# For GitHub Actions we will authenticate beforehand; locally, ensure ee.Initialize() works.
# -------------------------
# -------------------------
# INITIALIZE EARTH ENGINE
# -------------------------
# GitHub Actions: authenticate.py already ran ee.Initialize(...)
# Local machine: allow normal ee.Initialize() without browser fallback

try:
    ee.Initialize()
    print("✓ EE initialized")
except Exception as e:
    raise RuntimeError("❌ EE failed to initialize. Did you run authenticate.py locally?") from e

# -------------------------
# CONFIG
# -------------------------
REPO_NAME = "Anfu4354/pm-monitor-data"
OUT_DIR = "data"
OUT_CURRENT = f"{OUT_DIR}/current.json"
OUT_ANNUAL_PM25 = f"{OUT_DIR}/annual_pm25.json"
OUT_ANNUAL_TEMP = f"{OUT_DIR}/annual_temperature.json"
OUT_HISTORY = f"{OUT_DIR}/history.json"
HISTORY_LOCAL = "history.json"   # local copy we update and then push

# ROI (Monterrey)
LAT = 25.6866
LON = -100.3161
POINT = ee.Geometry.Point([LON, LAT])

# buffer (meters) for area reductions if needed
BUFFER_M = 20000

# coarse scale to speed up reduceRegion (meters)
REDUCE_SCALE = 5000

# history length (samples)
HISTORY_MAX = 200

# -------------------------
# UTIL FUNCTIONS
# -------------------------
def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z"

def safe_reduce_mean(img, geometry=POINT, scale=REDUCE_SCALE):
    """
    Try to compute mean value over geometry using a safe approach:
    set default projection, reduceRegion(bestEffort=True).
    Returns a dict (band->value) or None.
    """
    try:
        # Ensure image has a default projection at approx 'scale'
        proj = ee.Projection("EPSG:4326").atScale(scale)
        img2 = img.setDefaultProjection(proj)
        # mean aggregator may be a single-band image; do reduceRegion
        rr = img2.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geometry,
            scale=scale,
            bestEffort=True,
            maxPixels=1e13
        )
        return rr.getInfo()  # returns dict or {}
    except Exception as e:
        print("⚠ safe_reduce_mean failed:", e)
        return None

# -------------------------
# 1) CURRENT (last 24h)
# -------------------------
now = datetime.datetime.now(datetime.timezone.utc)
ee_now = ee.Date(now.isoformat())
ee_24h = ee_now.advance(-1, "day")

# PM2.5 — CAMS NRT (band used in earlier scripts)
cams_nrt = ee.ImageCollection("ECMWF/CAMS/NRT") \
    .select("particulate_matter_d_less_than_25_um_surface") \
    .filterBounds(POINT.buffer(BUFFER_M)) \
    .filterDate(ee_24h, ee_now)

pm25_img_now = cams_nrt.mean()
pm25_sample_now = safe_reduce_mean(pm25_img_now)
pm25_now_ug = None
if pm25_sample_now:
    k = list(pm25_sample_now.keys())[0]
    v = pm25_sample_now.get(k)
    pm25_now_ug = v * 1e9 if v is not None else None

# Temperature — ERA5-Land hourly (pick latest image or mean last few hours)
era5_hourly = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY") \
    .select("temperature_2m") \
    .filterBounds(POINT.buffer(BUFFER_M))

latest_temp_img = era5_hourly.sort("system:time_start", False).first()
temp_sample_now = safe_reduce_mean(latest_temp_img)
temp_now_c = None
if temp_sample_now:
    k = list(temp_sample_now.keys())[0]
    v = temp_sample_now.get(k)
    temp_now_c = v - 273.15 if v is not None else None

current_json = {
    "timestamp_utc": now_iso(),
    "pm25_ugm3": pm25_now_ug,
    "temperature_c": temp_now_c
}

# -------------------------
# 2) ANNUAL (last 365 days)
# -------------------------
ee_1y = ee_now.advance(-365, "day")

# Annual PM2.5 (CAMS over last 365 days)
cams_365 = ee.ImageCollection("ECMWF/CAMS/NRT") \
    .select("particulate_matter_d_less_than_25_um_surface") \
    .filterBounds(POINT.buffer(BUFFER_M)) \
    .filterDate(ee_1y, ee_now)

cams_365_mean = cams_365.mean()
annual_pm25_sample = safe_reduce_mean(cams_365_mean)
annual_pm25_ug = None
if annual_pm25_sample:
    k = list(annual_pm25_sample.keys())[0]
    v = annual_pm25_sample.get(k)
    annual_pm25_ug = v * 1e9 if v is not None else None

annual_pm25_json = {
    "pm25_ugm3": annual_pm25_ug,
    "dataset": "CAMS NRT (annual mean over last 365 days)",
    "generated_utc": now_iso()
}

# Annual Temperature (ERA5-Land mean over last 365 days)
era5_365 = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY") \
    .select("temperature_2m") \
    .filterBounds(POINT.buffer(BUFFER_M)) \
    .filterDate(ee_1y, ee_now)

era5_365_mean = era5_365.mean()
annual_temp_sample = safe_reduce_mean(era5_365_mean)
annual_temp_c = None
if annual_temp_sample:
    k = list(annual_temp_sample.keys())[0]
    v = annual_temp_sample.get(k)
    annual_temp_c = v - 273.15 if v is not None else None

annual_temp_json = {
    "temperature_c": annual_temp_c,
    "dataset": "ERA5-Land (annual mean over last 365 days)",
    "generated_utc": now_iso()
}

# -------------------------
# 3) HISTORY (append & keep rolling)
# -------------------------
def read_local_history(path=HISTORY_LOCAL):
    try:
        with open(path, "r", encoding="utf8") as fh:
            return json.load(fh)
    except Exception:
        return []

def append_history(local_path, current_obj, maxlen=HISTORY_MAX):
    hist = read_local_history(local_path)
    hist.append({
        "ts": current_obj["timestamp_utc"],
        "pm25": current_obj["pm25_ugm3"],
        "temp": current_obj["temperature_c"]
    })
    hist = hist[-maxlen:]
    with open(local_path, "w", encoding="utf8") as fh:
        json.dump(hist, fh, indent=2, ensure_ascii=False)
    return hist

history = append_history(HISTORY_LOCAL, current_json, maxlen=HISTORY_MAX)
history_json = history

# -------------------------
# 4) WRITE LOCAL FILES (temporary)
# -------------------------
def write_local(fname, obj):
    with open(fname, "w", encoding="utf8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)

write_local("current.json", current_json)
write_local("annual_pm25.json", annual_pm25_json)
write_local("annual_temperature.json", annual_temp_json)
write_local(HISTORY_LOCAL, history_json)
print("✓ Local files written")

# -------------------------
# 5) UPLOAD TO GITHUB
# -------------------------
TOKEN = os.getenv("GITHUB_TOKEN")
if not TOKEN:
    raise RuntimeError("GITHUB_TOKEN environment variable not set.")

g = Github(auth=Auth.Token(TOKEN))
repo = g.get_repo(REPO_NAME)

def upload_json(path_in_repo, local_file):
    with open(local_file, "r", encoding="utf8") as fh:
        content = fh.read()
    try:
        contents = repo.get_contents(path_in_repo)
        repo.update_file(contents.path, f"Auto-update {os.path.basename(path_in_repo)}", content, contents.sha)
        print("✓ Updated", path_in_repo)
    except Exception as e:
        try:
            repo.create_file(path_in_repo, f"Create {os.path.basename(path_in_repo)}", content)
            print("✓ Created", path_in_repo)
        except Exception as e2:
            print("✖ Failed to upload", path_in_repo, ":", e2)

upload_json(OUT_CURRENT, "current.json")
upload_json(OUT_ANNUAL_PM25, "annual_pm25.json")
upload_json(OUT_ANNUAL_TEMP, "annual_temperature.json")
upload_json(OUT_HISTORY, HISTORY_LOCAL)

print("🎉 All files uploaded.")

