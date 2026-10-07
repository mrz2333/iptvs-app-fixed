from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import time
import os
import sys
import re
import gc
import threading
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

VERSION = "1.0.0"

if os.name != 'nt':  # Only on Unix-like systems (Docker usually runs Linux)
    os.environ['TZ'] = 'Asia/Shanghai'
    try:
        time.tzset()
    except Exception:
        pass

app = Flask(__name__)

# Constants
API_URL = "https://iptvs.pes.im"
# 缓存与配置统一落到挂载卷（WB_DATA_DIR 或默认 /app/data），容器重启后可直接读回，
# 无需重新拉取+测速。若目录不可写则回退到当前工作目录。
def _data_path(name):
    base = os.environ.get("IPTV_DATA_DIR", "/app/data")
    try:
        os.makedirs(base, exist_ok=True)
        return os.path.join(base, name)
    except Exception:
        return name

CACHE_FILE = _data_path("iptv_sources.m3u8")
TXT_CACHE_FILE = _data_path("iptv_sources.txt")
CHANNEL_LIST_FILE = os.path.join(os.path.dirname(CACHE_FILE), "channel_list.txt")
ADDRESS_LIST_FILE = os.path.join(os.path.dirname(CACHE_FILE), "address_list.txt")
HSMD_ADDRESS_LIST_FILE = os.path.join(os.path.dirname(CACHE_FILE), "hsmd_address_list.txt")
HSMD_PROBE_MAX = 30  # hsmdtv 自动探测的最大频道号
ZHGXTV_INTERFACE = "/ZHGXTV/Public/json/live_interface.txt"
TXIPTV_TEST_URI = "/tsfile/live/0001_1.m3u8"
HSMDTV_TEST_URI = "/newlive/live/hls/1/live.m3u8"
MAX_WORKERS = 20
TOP_N = 5

global_m3u8_content = ""
global_txt_content = ""
last_run_time = "Never"
is_running = False
# 全局锁：防止 /forceRetest 与定时任务并发执行 scheduled_task 造成内存翻倍
_task_lock = threading.Lock()

def get_standard_channel_map():
    """Returns a dict mapping 'normalized' names to standard names from channel_list.txt."""
    mapping = {}
    try:
        if os.path.exists(CHANNEL_LIST_FILE):
            with open(CHANNEL_LIST_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    std_name = line.strip()
                    if not std_name: continue
                    # Normalize key: remove hyphens, spaces, uppercase
                    key = std_name.replace('-', '').replace(' ', '').upper()
                    # Also handle CCTV1 -> CCTV-1 specifically if standard is CCTV-1
                    # Actually standard name IS the value. Key is normalization.
                    mapping[key] = std_name
    except Exception as e:
        print(f"Error loading channel map: {e}")
    return mapping

def map_to_standard_name(name, mapping):
    """Maps a potentially variant name to a standard one if matches."""
    key = name.replace('-', '').replace(' ', '').upper()
    return mapping.get(key, name)

def fetch_api_data():
    """Fetches JSON data from the API with retry logic."""
    for attempt in range(3):
        try:
            print(f"Fetching API data (Attempt {attempt+1})...")
            # 用 with 确保响应体关闭，避免连接/缓冲区泄漏
            with requests.get(API_URL, timeout=10) as response:
                if response.status_code == 200:
                    print("API data fetched successfully.")
                    return response.json()
        except Exception as e:
            print(f"API fetch error: {e}")
        time.sleep(5)
    print("API fetch failed after re-tries.")
    return []

def get_download_speed(url):
    """
    Measures download speed of a given URL (usually a TS file).
    Returns speed in MB/s. Returns -1 if failed.
    """
    try:
        start_time = time.time()
        # Download first 512KB is usually enough for speed test, but here let's follow ZHGXTV full download logic or chunk
        # ZHGXTV uses content length / time. Let's limit read size to avoid huge files.
        # But TS files are small chunks usually.
        with requests.get(url, stream=True, timeout=10) as r:
            r.raise_for_status()
            size = 0
            # Read at most 10MB for test to ensure accuracy over longer time
            chunk_size = 8192
            limit_size = 10 * 1024 * 1024
            
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    size += len(chunk)
                # Ensure we download enough data or for enough time
                if size > limit_size:
                    break
                # Or if time exceeds 8 seconds
                if time.time() - start_time > 8:
                    break
        
        duration = time.time() - start_time
        if duration == 0: duration = 0.001
        
        speed = (size / 1024 / 1024) / duration # MB/s
        return speed
    except Exception:
        return -1

def get_ts_url(m3u8_url):
    """
    Parses m3u8 to find the first TS segment URL.
    Returns the full TS URL.
    """
    try:
        with requests.get(m3u8_url, timeout=5) as response:
            if response.status_code != 200:
                return None
            
            lines = response.text.strip().split('\n')
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):
                # Handle relative or absolute URLs
                if line.startswith('http'):
                    return line
                elif line.startswith('/'):
                    # Absolute path
                    base = m3u8_url.split('/')[0] + "//" + m3u8_url.split('/')[2]
                    return base + line
                else:
                    # Relative path
                    base = m3u8_url.rsplit('/', 1)[0]
                    return base + "/" + line
        return None
    except:
        return None

def test_host_speed(item):
    """
    Tests speed for a single host item.
    Returns { ...item, 'speed': speed_mb_s, 'channels': [optional_list_for_zhgxtv] }
    """
    host = item.get('host')
    match_type = item.get('matchType')
    
    if not host:
        return -1, None
        
    speed = -1
    channels = []

    try:
        if match_type == 'txiptv':
            # Updated logic to use JSON API like iptv.py
            json_url = f"http://{host}/iptv/live/1000.json?key=txiptv"
            try:
                # Use short timeout for JSON fetch as per iptv.py logic (0.5s there, maybe generic 2s here)
                response = requests.get(json_url, timeout=2)
                try:
                    ok = response.status_code == 200
                    json_data = response.json() if ok else None
                finally:
                    response.close()
                if ok:
                    valid_channel_url = None
                    
                    if 'data' in json_data:
                        for item in json_data['data']:
                            if isinstance(item, dict):
                                name = item.get('name')
                                urlx = item.get('url')
                                
                                if not name or not urlx:
                                    continue
                                
                                if ',' in urlx:
                                    continue

                                full_url = ""
                                if 'http' in urlx:
                                    full_url = urlx
                                else:
                                    if urlx.startswith('/'):
                                        full_url = f"http://{host}{urlx}" 
                                    else:
                                        full_url = f"http://{host}/{urlx}"

                                channels.append({'name': name, 'url': full_url})
                                
                                if not valid_channel_url:
                                    valid_channel_url = full_url

                    if valid_channel_url:
                        ts_url = get_ts_url(valid_channel_url)
                        if ts_url:
                            speed = get_download_speed(ts_url)
                    else:
                        speed = -1
                else:
                    speed = -1
            except Exception as e:
                # print(f"TXIPTV JSON fetch failed for {host}: {e}")
                speed = -1
        
        elif match_type == 'hsmdtv':
            test_url = f"http://{host}{HSMDTV_TEST_URI}"
            ts_url = get_ts_url(test_url)
            if ts_url:
                speed = get_download_speed(ts_url)
        
        elif match_type == 'jsmpeg':
            # jsmpeg logic from all-z-j-new.py
            json_url = f"http://{host}/streamer/list"
            try:
                response = requests.get(json_url, timeout=2)
                try:
                    ok = response.status_code == 200
                    json_data = response.json() if ok else None
                finally:
                    response.close()
                if ok:
                    valid_channel_url = None
                    for item in json_data:
                        name = item.get('name', '').strip()
                        key = item.get('key', '').strip()
                        if not name or not key:
                            continue
                        
                        full_url = f"http://{host}/hls/{key}/index.m3u8"
                        channels.append({'name': name, 'url': full_url})
                        
                        # Use the first valid channel for speed test
                        if not valid_channel_url:
                            valid_channel_url = full_url

                    if valid_channel_url:
                        ts_url = get_ts_url(valid_channel_url)
                        if ts_url:
                            speed = get_download_speed(ts_url)
                    else:
                        speed = -1
                else:
                    speed = -1
            except Exception as e:
                # print(f"JSMPEG fetch failed for {host}: {e}")
                speed = -1

        elif match_type == 'zhgxtv':
            # Referencing ZHGXTV.py: Fetch live_interface.txt first
            interface_url = f"http://{host}{ZHGXTV_INTERFACE}"
            target_response = requests.get(interface_url, timeout=5)
            try:
                ok = target_response.status_code == 200
                content = target_response.content.decode('utf-8', errors='ignore') if ok else ""
            finally:
                target_response.close()
            if ok:
                lines = content.split('\n')
                
                valid_channel_url = None
                
                # Parse channels here to save for later use (avoid re-fetching)
                for line in lines:
                    line = line.strip()
                    if ',' in line:
                        parts = line.split(',')
                        if len(parts) >= 2:
                            name = parts[0].strip()
                            url_part = parts[1].strip()
                            
                            # Reconstruct URL as per logic
                            
                            try:
                                full_url = ""
                                if url_part.startswith("http"):
                                    # Parse and replace host
                                    p = urlparse(url_part)
                                    # Reconstruct: scheme + netloc(host) + path + params + query + fragment
                                    # Since we want to use the current 'host' (which is ip:port)
                                    full_url = f"{p.scheme}://{host}{p.path}"
                                    if p.query:
                                        full_url += f"?{p.query}"
                                elif url_part.startswith("/"):
                                    full_url = f"http://{host}{url_part}"
                                else:
                                    full_url = f"http://{host}/{url_part}"
                                
                                channels.append({'name': name, 'url': full_url})
                                
                                if not valid_channel_url:
                                    valid_channel_url = full_url
                            
                            except Exception as e:
                                print(f"Error parsing line {line}: {e}")
                                continue
                if valid_channel_url:
                    ts_url = get_ts_url(valid_channel_url)
                    if ts_url:
                        speed = get_download_speed(ts_url)
                else:
                    speed = -1 # No valid channels found
            else:
                speed = -1

    except Exception as e:
        # print(f"Speed test failed for {host}: {e}")
        speed = -1
        
    return speed, channels

def clean_channel_name(name):
    """Clean and normalize channel name."""
    name = name.replace("cctv", "CCTV")
    name = name.replace("中央", "CCTV")
    name = name.replace("央视", "CCTV")
    for rep in ["高清", "超高", "HD", "标清", "频道", "-", " ", "PLUS", "＋", "(", ")"]:
        name = name.replace(rep, "" if rep not in ["PLUS", "＋"] else "+")
    name = re.sub(r"CCTV(\d+)台", r"CCTV\1", name)
    name_map = {
        "CCTV1综合": "CCTV1", "CCTV2财经": "CCTV2", "CCTV3综艺": "CCTV3", "CCTV4国际": "CCTV4",
        "CCTV4中文国际": "CCTV4", "CCTV4欧洲": "CCTV4", "CCTV5体育": "CCTV5", "CCTV6电影": "CCTV6",
        "CCTV7军事": "CCTV7", "CCTV7军农": "CCTV7", "CCTV7农业": "CCTV7", "CCTV7国防军事": "CCTV7",
        "CCTV8电视剧": "CCTV8", "CCTV9记录": "CCTV9", "CCTV9纪录": "CCTV9", "CCTV10科教": "CCTV10",
        "CCTV11戏曲": "CCTV11", "CCTV12社会与法": "CCTV12", "CCTV13新闻": "CCTV13", "CCTV新闻": "CCTV13",
        "CCTV14少儿": "CCTV14", "CCTV15音乐": "CCTV15", "CCTV16奥林匹克": "CCTV16",
        "CCTV17农业农村": "CCTV17", "CCTV17农业": "CCTV17", "CCTV5+体育赛视": "CCTV5+",
        "CCTV5+体育赛事": "CCTV5+", "CCTV5+体育": "CCTV5+", "CCTV01": "CCTV1", "CCTV02": "CCTV2", "CCTV03": "CCTV3", "CCTV04": "CCTV4",
        "CCTV05": "CCTV5", "CCTV06": "CCTV6", "CCTV07": "CCTV7", "CCTV08": "CCTV8", "CCTV09": "CCTV9"
    }
    name = name_map.get(name, name)
    return name

def process_txiptv_channels(channels, source_label, source_index):
    """Generates m3u8 entries for txiptv source using JSON parsing logic."""
    entries = []
    std_map = get_standard_channel_map()
    
    try:
        if not channels: return []
        for ch in channels:
            name = ch['name']
            url = ch['url']
            
            # Name cleaning 
            name = clean_channel_name(name)
            
            # Standardization
            name = map_to_standard_name(name, std_map)
            
            entries.append({'name': name, 'url': url, 'content': f'#EXTINF:-1 group-title="IPTV",{name}\n{url}', 'index': source_index})
            
    except Exception as e:
        print(f"Error processing txiptv channels: {e}")
    return entries

def probe_hsmdtv_channels(host, max_channel=HSMD_PROBE_MAX):
    """
    当 hsmd_address_list.txt 缺失时，自动探测 hsmdtv 频道号。
    hsmdtv 的单频道流地址形如 /newlive/live/hls/{N}/live.m3u8。
    通过 HEAD/GET 探测哪些频道号可用，生成 (频道名, 路径) 列表。
    仅探测一次并缓存到文件，避免每次都扫。
    """
    found = []
    for n in range(1, max_channel + 1):
        uri = f"/newlive/live/hls/{n}/live.m3u8"
        url = f"http://{host}{uri}"
        try:
            with requests.get(url, timeout=2, stream=True) as r:
                if r.status_code != 200:
                    continue
                # 只读一点点确认是 m3u8
                head = r.raw.read(64, decode_content=True) or b""
                if b"#EXTM3U" in head:
                    found.append((f"频道{n}", uri))
        except Exception:
            continue
    return found


def process_hsmdtv_channels(host, source_label, source_index):
    """
    Generates m3u8 entries for hsmdtv source.
    优先读 hsmd_address_list.txt；文件缺失时自动探测频道号（结果写回该文件作为缓存）。
    """
    entries = []
    std_map = get_standard_channel_map()
    try:
        lines = None
        # 读缓存文件；若文件里的 host 与当前 host 不一致（源换了），则视为失效并重探，
        # 避免"源换 host 后仍用旧地址"导致该源全废。
        if os.path.exists(HSMD_ADDRESS_LIST_FILE):
            with open(HSMD_ADDRESS_LIST_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            cached_host = None
            for _l in lines:
                _m = re.search(r'http://([^\s/]+)', _l)
                if _m:
                    cached_host = _m.group(1)
                    break
            if cached_host and cached_host != host:
                print(f"hsmdtv: cached host {cached_host} != current {host}, re-probing.")
                lines = None

        if not lines:
            # 文件缺失或 host 已变：自动探测并生成（自给自足，避免每次刷 "not found" 日志）
            probed = probe_hsmdtv_channels(host)
            if probed:
                lines = [f"hsmd-{name.replace('频道','')} http://{host}{uri}\n" for name, uri in probed]
                try:
                    with open(HSMD_ADDRESS_LIST_FILE, 'w', encoding='utf-8') as f:
                        f.writelines(lines)
                    print(f"Auto-generated {HSMD_ADDRESS_LIST_FILE} ({len(lines)} channels) from host {host}")
                except Exception as we:
                    print(f"Could not cache {HSMD_ADDRESS_LIST_FILE}: {we}")
            else:
                # 探测不到就静默跳过（原版会每次刷 not found）
                print(f"hsmdtv: no channels discovered from {host}, skipped.")
                return []

        for line in lines:
            line = line.strip()
            if not line: continue
            
            # Find HTTP URL
            match = re.search(r'(http://[^\s]+)', line)
            if match:
                url_in_file = match.group(1)
                
                # Extract Name: everything before URL
                part_before_url = line.split(url_in_file)[0]
                # Remove ID (digits) at start
                name = re.sub(r'^\s*\d+\s+', '', part_before_url).strip()
                # Clean name
                name = name.replace("（默认频道）", "").strip()
                name = clean_channel_name(name)
                
                # Standardization
                name = map_to_standard_name(name, std_map)
                
                parsed = urlparse(url_in_file)
                new_url = f"http://{host}{parsed.path}"
                
                entries.append({
                    'name': name,
                    'url': new_url,
                    'content': f'#EXTINF:-1 group-title="IPTV",{name}\n{new_url}',
                    'index': source_index
                })
    except Exception as e:
        print(f"Error processing hsmdtv channels: {e}")
    return entries

def process_zhgxtv_channels(channels, source_label, source_index):
    """Generates m3u8 entries for zhgxtv source."""
    entries = []
    std_map = get_standard_channel_map()
    if not channels: return []
    for ch in channels:
        name = ch['name']
        url = ch['url']
        
        # Cleanup name
        name = clean_channel_name(name)
        
        # Standardization
        name = map_to_standard_name(name, std_map)
        
        entries.append({'name': name, 'url': url, 'content': f'#EXTINF:-1 group-title="IPTV",{name}\n{url}', 'index': source_index})
    return entries

def process_jsmpeg_channels(channels, source_label, source_index):
    """Generates m3u8 entries for jsmpeg source."""
    entries = []
    std_map = get_standard_channel_map()
    
    try:
        for ch in channels:
            name = ch['name']
            url = ch['url']
            
            # Clean name
            name = clean_channel_name(name)
            
            # Standardization
            name = map_to_standard_name(name, std_map)
            
            entries.append({'name': name, 'url': url, 'content': f'#EXTINF:-1 group-title="IPTV",{name}\n{url}', 'index': source_index})
            
    except Exception as e:
        print(f"Error processing jsmpeg channels: {e}")
    return entries

def channel_sort_key(name):
    """
    Sort key for channel names.
    Order: CCTV-X, CCTV-others, Satellite (卫视), Others.
    """
    name_upper = name.upper()
    
    # CCTV channels
    if "CCTV" in name_upper:
        # Extract number if present
        match = re.search(r"CCTV(\d+)", name_upper)
        if match:
            num = int(match.group(1))
            return (0, num)
        elif "5+" in name_upper:
             return (0, 5.5) # Place between 5 and 6
        else:
            # CCTV News, etc. Place after numbered CCTVs
            return (0, 999)
            
    # Satellite TV (卫视)
    if "卫视" in name:
        return (1, name)
        
    return (2, name)

def scheduled_task():
    global global_m3u8_content, global_txt_content, last_run_time, is_running
    # 加锁 + 重入检查：定时任务与 /forceRetest 手动触发并发时，第二个直接跳过，
    # 防止多份完整数据副本同时在内存中导致消耗翻倍。
    if not _task_lock.acquire(blocking=False):
        print("Task already running, skip this trigger.")
        return
    try:
        _run_scheduled_task()
    finally:
        _task_lock.release()
        # 任务结束主动回收内存：
        # 1) gc.collect() 清理 Python 对象引用
        # 2) malloc_trim(0) 把 glibc 空闲堆真正还给操作系统 —— 否则即使 gc 了，
        #    进程 RSS 也不会下降（这是本服务"跑完测速内存不回落"的关键）
        gc.collect()
        try:
            import ctypes
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass


def _run_scheduled_task():
    global global_m3u8_content, global_txt_content, last_run_time, is_running
    is_running = True
    print("Executing scheduled task...")
    last_run_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    
    data = fetch_api_data()
    
    if not data or not isinstance(data, dict) or "results" not in data:
        print("No valid data received or 'results' key missing.")
        is_running = False
        return

    result = data["results"]
    
    if not result:
        print("No data in result.")
        is_running = False
        return

    # Speed test in parallel
    results_with_speed = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_item = {executor.submit(test_host_speed, item): item for item in result}
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                speed, channels = future.result()
                # 内存优化：channels 可能包含上百个频道的 name+url，只有最终入选
                # top_sources 的源才会用到它。这里先只保留轻量的测速结果，
                # channels 放回 future 引用、用完随 future 一起释放，
                # 避免所有源的频道数据同时驻留内存。
                if speed > 0:
                    print(f"Host {item['host']} Speed: {speed:.2f} MB/s matchType: {item['matchType']} source: {item.get('source', 'N/A')}")
                    results_with_speed.append({
                        'host': item['host'],
                        'matchType': item['matchType'],
                        'speed': speed,
                        'channels': channels
                    })
                else:
                    # 慢速源的频道数据不保留
                    channels = None
            except Exception as e:
                print(f"Error testing {item['host']}: {e}")
    # 释放测速阶段的引用集合与线程池结果包装
    del future_to_item

    # Sort and pick top N
    
    # Filter by speed limit 2MB/s
    valid_results = [r for r in results_with_speed if r['speed'] > 1.5]
    
    # Sort by speed descending
    valid_results.sort(key=lambda x: x['speed'], reverse=True)
    
    final_sources = []
    selected_hosts = set()
    
    # Ensure at least one from each type if available and fast enough
    required_matches = ['txiptv', 'hsmdtv', 'zhgxtv', 'jsmpeg']
    
    for m in required_matches:
        # Find best for this match type
        for res in valid_results:
            if res['matchType'] == m and res['host'] not in selected_hosts:
                final_sources.append(res)
                selected_hosts.add(res['host'])
                break # Only need one per type for now to ensure diversity
    
    # Fill the rest with optimal speed from remaining
    for res in valid_results:
        if len(final_sources) >= TOP_N:
            break
        if res['host'] not in selected_hosts:
             final_sources.append(res)
             selected_hosts.add(res['host'])

    # Re-sort final collection by speed
    final_sources.sort(key=lambda x: x['speed'], reverse=True)
    
    top_sources = final_sources
    
    print(f"Selected top {len(top_sources)} sources.")

    if len(top_sources) < 3:
        print(f"Not enough sources found ({len(top_sources)} < 3).")
        
        # Load from file if empty in memory
        if not global_m3u8_content and os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                global_m3u8_content = f.read()
        if not global_txt_content and os.path.exists(TXT_CACHE_FILE):
            with open(TXT_CACHE_FILE, "r", encoding="utf-8") as f:
                global_txt_content = f.read()
                
        update_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        dummy_name = f"更新时间: {update_time_str}"
        dummy_url = "http://127.0.0.1/"
        
        if global_m3u8_content:
            lines = global_m3u8_content.split('\n')
            for i, line in enumerate(lines):
                if line.startswith("#EXT-X-UPDATED:"):
                    lines[i] = f"#EXT-X-UPDATED: {update_time_str}"
                elif line.startswith('#EXTINF:-1 group-title="Update",更新时间:'):
                    lines[i] = f'#EXTINF:-1 group-title="Update",{dummy_name}'
            global_m3u8_content = "\n".join(lines)
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                f.write(global_m3u8_content)
                
        if global_txt_content:
            lines = global_txt_content.split('\n')
            for i, line in enumerate(lines):
                if line.startswith("更新时间:"):
                    lines[i] = f"{dummy_name},{dummy_url}"
                    break
            global_txt_content = "\n".join(lines)
            with open(TXT_CACHE_FILE, "w", encoding="utf-8") as f:
                f.write(global_txt_content)
                
        is_running = False
        return

    # Collect all entries
    all_entries = []
    
    for idx, source in enumerate(top_sources):
        speed_str = f"{source['speed']:.2f}MB/s"
        console_label = f"源{idx+1} {speed_str}"
        m3u8_label = f"源{idx+1}"
        print(f"Processing {console_label}: {source['host']} ({source['matchType']}) {source.get('source', 'N/A')}")
        
        if source['matchType'] == 'txiptv':
            entries = process_txiptv_channels(source['channels'], m3u8_label, idx)
            all_entries.extend(entries)
        elif source['matchType'] == 'hsmdtv':
            entries = process_hsmdtv_channels(source['host'], m3u8_label, idx)
            all_entries.extend(entries)
        elif source['matchType'] == 'zhgxtv':
            entries = process_zhgxtv_channels(source['channels'], m3u8_label, idx)
            all_entries.extend(entries)
        elif source['matchType'] == 'jsmpeg':
            entries = process_jsmpeg_channels(source['channels'], m3u8_label, idx)
            all_entries.extend(entries)

    # Group by name
    # We want to keep the order of channel names as much as possible
    grouped_entries = {}
    channel_order = [] 
    
    # Pre-populate order from channel_list.txt if we want specific order for those
    try:
        with open(CHANNEL_LIST_FILE, 'r', encoding='utf-8') as f:
            for l in f.readlines():
                name = l.strip()
                if name:
                    grouped_entries[name] = []
                    channel_order.append(name)
    except:
        pass

    for entry in all_entries:
        name = entry['name']
        if name not in grouped_entries:
            grouped_entries[name] = []
        grouped_entries[name].append(entry)

    # Sort the channel names
    
    # Pre-populate order from channel_list.txt if we want specific order for those
    # Actually, user wants standard sorting now. Let's prioritize standard sorting but also enable manual order if provided.
    
    # Get all unique channel names
    unique_channel_names = list(grouped_entries.keys())
    
    # Custom sort function
    def sort_channels(name):
        return channel_sort_key(name)
    
    unique_channel_names.sort(key=sort_channels)
    
    channel_order = unique_channel_names

    # Insert dummy update time channel at the top
    update_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    dummy_name = f"更新时间: {update_time_str}"
    dummy_url = "http://127.0.0.1/" # Dummy URL
    
    # Add to m3u8 lines
    m3u8_lines = ["#EXTM3U", f"#EXT-X-UPDATED: {update_time_str}"]
    m3u8_lines.append(f'#EXTINF:-1 group-title="Update",{dummy_name}\n{dummy_url}')
    
    for name in channel_order:
        entries_list = grouped_entries.get(name, [])
        # Sort by source index to ensure 源1, 源2 order
        entries_list.sort(key=lambda x: x['index'])
        if entries_list:  # Only add header if sources exist (optional, m3u8 format usually implies listing)
            for entry in entries_list:
                 m3u8_lines.append(entry['content'])

    global_m3u8_content = "\n".join(m3u8_lines)
    
    # Save to file (optional, but good for persistence)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        f.write(global_m3u8_content)

    # Generate TXT content
    txt_lines = []
    
    # Add dummy first
    txt_lines.append(f"{dummy_name},{dummy_url}")
    
    # Use grouped_entries to iterate channels in order (same order as m3u8)
    # We iterate channel_order, which contains unique names
    unique_names_processed = set()
    
    for name in channel_order:
        if name in unique_names_processed:
            continue
        unique_names_processed.add(name)
        
        entries_list = grouped_entries.get(name, [])
        # Sort by source index
        entries_list.sort(key=lambda x: x.get('index', 999))
        
        for entry in entries_list:
            if 'url' in entry:
                txt_lines.append(f"{entry['name']},{entry['url']}")
    
    global_txt_content = "\n".join(txt_lines)
    
    # Save to file
    with open(TXT_CACHE_FILE, "w", encoding="utf-8") as f:
        f.write(global_txt_content)
        
    print(f"M3U8 and TXT generation complete at {last_run_time}.")
    is_running = False

@app.route('/txt')
def get_txt():
    global global_txt_content
    # Try to load from file if memory is empty (after restart)
    if not global_txt_content and os.path.exists(TXT_CACHE_FILE):
        with open(TXT_CACHE_FILE, "r", encoding="utf-8") as f:
            global_txt_content = f.read()

    if not global_txt_content:
        return "Not ready yet. Please wait for the first scan.", 503
        
    return global_txt_content, 200, {'Content-Type': 'text/plain; charset=utf-8'}


@app.route('/')
def status():
    return jsonify({
        "status": "running" if not is_running else "updating",
        "last_run": last_run_time,
        "message": "Visit /iptv for m3u8 playlist, /txt for text playlist."
    })

@app.route('/iptv')
def get_m3u8():
    global global_m3u8_content
    # Try to load from file if memory is empty (after restart)
    if not global_m3u8_content and os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            global_m3u8_content = f.read()

    if not global_m3u8_content:
        return "Not ready yet. Please wait for the first scan.", 503
        
    return global_m3u8_content, 200, {'Content-Type': 'application/vnd.apple.mpegurl'}

@app.route('/forceRetest')
def force_retest():
    global is_running
    # 用锁做原子判断，避免「检查-启动」之间的竞态窗口里被连点出多个线程
    if is_running or _task_lock.locked():
         return jsonify({"message": "Update already in progress.", "status": "busy"}), 429
         
    t = threading.Thread(target=scheduled_task, daemon=True)
    t.start()
    return jsonify({"message": "Force retest started in background.", "status": "started"})

# Initial run in background on startup (or trigger manually)
def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=scheduled_task, trigger="interval", hours=12)
    scheduler.start()
    
    # Run immediately in a separate thread to not block startup
    threading.Thread(target=scheduled_task, daemon=True).start()

if __name__ == '__main__':
    print(f"Starting... - Version: {VERSION}")
    start_scheduler()
    app.run(host='0.0.0.0', port=5000)

