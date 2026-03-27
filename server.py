import http.server
import json
import sys
import os
import urllib.request
import glob
import shutil
import zipfile
import mimetypes
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup  # Added from your script

PORT = 8000
AUDIO_DIR = "audios"
CONFIG_FILE = "config.json"

# ========================================================
# 1. CONFIGURATION MANAGER
# ========================================================
def load_config():
    # 1. Check Docker Environment Variables first
    env_url = os.environ.get("ABS_URL")
    env_token = os.environ.get("ABS_TOKEN")
    
    if env_url and env_token:
        print("[!] Using configuration from Docker Environment Variables.")
        return {"ABS_URL": env_url, "ABS_TOKEN": env_token}
        
    # 2. Fallback to config.json if not running in Docker with env vars
    if not os.path.exists(CONFIG_FILE):
        default_config = {
            "ABS_URL": "http://192.168.0.7:13378",
            "ABS_TOKEN": "PASTE_YOUR_TOKEN_HERE"
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(default_config, f, indent=4)
        print(f"\n[!] Created '{CONFIG_FILE}'.")
        print(f"[!] Please open {CONFIG_FILE}, paste your API token, or set ENV variables.\n")
        return default_config
        
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

config = load_config()
ABS_URL = config.get("ABS_URL", "").strip().rstrip('/')
ABS_TOKEN = config.get("ABS_TOKEN", "").strip()

if not os.path.exists(AUDIO_DIR):
    os.makedirs(AUDIO_DIR)
    print(f"[*] Created '{AUDIO_DIR}' folder for local files.")


# ========================================================
# 2. EPUB TO SRT PROCESSOR LOGIC
# ========================================================
def time_to_srt(time_string):
    """Converts EPUB SMIL time formats to standard SRT timestamp format."""
    time_string = time_string.replace('s', '').strip()
    if ':' in time_string:
        parts = time_string.split(':')
        if len(parts) == 3:
            h, m, s = float(parts[0]), float(parts[1]), float(parts[2])
        else:
            h, m, s = 0, float(parts[0]), float(parts[1])
    else:
        h, m = 0, 0
        s = float(time_string)
    
    # Calculate milliseconds
    total_ms = int((h * 3600 + m * 60 + s) * 1000)
    hours = total_ms // 3600000
    minutes = (total_ms % 3600000) // 60000
    seconds = (total_ms % 60000) // 1000
    milliseconds = total_ms % 1000
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{milliseconds:03d}"

def convert_epub_to_srt(epub_path):
    """Reads the EPUB, extracts subtitles, and returns the SRT file as a string."""
    srt_content = []
    
    with zipfile.ZipFile(epub_path, 'r') as epub:
        # Find all the SMIL synchronization files
        smil_files = [f for f in epub.namelist() if f.endswith('.smil')]
        smil_files.sort() # Ensure they are processed in chronological order
        
        if not smil_files:
            raise Exception("No SMIL Media Overlay files found in this EPUB.")

        sub_index = 1
        for smil_file in smil_files:
            smil_data = epub.read(smil_file)
            smil_soup = BeautifulSoup(smil_data, 'xml')
            
            # Every <par> tag represents one synchronized text/audio fragment
            for par in smil_soup.find_all('par'):
                text_node = par.find('text')
                audio_node = par.find('audio')
                
                if text_node and audio_node:
                    start_time = time_to_srt(audio_node.get('clipBegin', '0s'))
                    end_time = time_to_srt(audio_node.get('clipEnd', '0s'))
                    
                    src = text_node.get('src', '')
                    if '#' in src:
                        html_file, element_id = src.split('#')
                        
                        base_dir = os.path.dirname(smil_file)
                        html_path = os.path.normpath(os.path.join(base_dir, html_file)).replace('\\', '/')
                        
                        try:
                            html_data = epub.read(html_path)
                            html_soup = BeautifulSoup(html_data, 'html.parser')
                            target_element = html_soup.find(id=element_id)
                            
                            if target_element:
                                text_content = target_element.get_text(strip=True)
                                srt_content.append(f"{sub_index}\n{start_time} --> {end_time}\n{text_content}\n")
                                sub_index += 1
                        except KeyError:
                            pass # File not found in zip, skip gracefully

    return "\n".join(srt_content)


# ========================================================
# 3. STATE MANAGER & HTTP HANDLER
# ========================================================
state = {
    "browser": {"time": 0.0, "duration": 0.0, "status": "pause", "speed": 1.0},
    "command": {"action": "none", "val": 0, "id": "0"}
}

class AudioHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_POST(self):
        """Handle File Uploads (for the EPUB to SRT converter)"""
        parsed = urlparse(self.path)
        
        if parsed.path == '/upload_epub':
            try:
                qs = parse_qs(parsed.query)
                filename = qs.get('filename', ['book.epub'])[0]
                content_length = int(self.headers.get('Content-Length', 0))
                
                if content_length == 0:
                    self.send_error(400, "Empty payload")
                    return
                    
                file_data = self.rfile.read(content_length)
                
                # Save uploaded file temporarily
                temp_epub_path = f"temp_{filename}"
                with open(temp_epub_path, 'wb') as f:
                    f.write(file_data)
                    
                # Process EPUB to extract SRT
                srt_string = convert_epub_to_srt(temp_epub_path)
                
                # Clean up temp file
                if os.path.exists(temp_epub_path):
                    os.remove(temp_epub_path)
                    
                # Send the generated SRT back to the client as a direct download
                self.send_response(200)
                self.send_header('Content-type', 'application/x-subrip')
                safe_filename = filename.replace('.epub', '') + '.srt'
                self.send_header('Content-Disposition', f'attachment; filename="{safe_filename}"')
                self.end_headers()
                
                self.wfile.write(srt_string.encode('utf-8'))
                
            except Exception as e:
                print(f"[ERROR] Failed EPUB processing: {e}")
                self.send_error(500, f"Error processing EPUB: {str(e)}")
                # Ensure cleanup on failure
                if 'temp_epub_path' in locals() and os.path.exists(temp_epub_path):
                    os.remove(temp_epub_path)
        else:
            self.send_error(404, "Not Found")

    def do_GET(self):
        global state
        try:
            parsed = urlparse(self.path)
            
            # --------------------------------------------------------
            # WEB INTERFACE
            # --------------------------------------------------------
            if parsed.path == '/':
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                
                html = """
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Kindle Audio Receiver</title>
                    <style>
                        body { text-align: center; margin-top: 20px; font-family: sans-serif; background-color: #eef2f5; color: #333; }
                        .container { background: white; padding: 25px; border-radius: 12px; box-shadow: 0 8px 16px rgba(0,0,0,0.1); display: inline-block; min-width: 550px; max-width: 800px; }
                        select, button, input[type="file"] { padding: 12px; font-size: 16px; margin: 8px 0; border-radius: 8px; border: 1px solid #ccc; cursor: pointer; width: 100%; box-sizing: border-box; }
                        button { background-color: #007bff; color: white; border: none; font-weight: bold; transition: 0.2s; }
                        button:hover { background-color: #0056b3; }
                        .radio-group { margin-bottom: 20px; font-size: 18px; font-weight: bold; background: #f8f9fa; padding: 15px; border-radius: 8px;}
                        .section { padding: 15px; margin-bottom: 20px; border-radius: 8px; background: #f0f4f8; }
                        #status { font-size: 18px; font-weight: bold; margin-top: 15px; padding: 15px; border-radius: 8px; background: #f4f4f4;}
                        
                        #cover_art { 
                            display: none; 
                            width: 200px; 
                            height: 200px; 
                            object-fit: cover; 
                            border-radius: 12px; 
                            box-shadow: 0 10px 20px rgba(0,0,0,0.15); 
                            margin: 10px auto;
                            border: 1px solid #ddd;
                        }
                        
                        .sub-controls { background: #fff; padding: 15px; border-radius: 8px; margin-top: 15px; border: 1px solid #ddd; display: none; }
                        .sub-controls label { font-weight: bold; display: block; margin-top: 10px; margin-bottom: 5px; text-align: left; font-size: 14px; color: #555;}
                        
                        /* Styles for EPUB Tools Section */
                        .epub-tools { background: #e2e8f0; border: 1px dashed #94a3b8; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h2>🎧 Pimba Kindle Audio Sync</h2>
                        
                        <div class="section epub-tools">
                            <h3 style="margin-top: 0; margin-bottom: 10px; color: #334155;">📖 Extract SRT from EPUB</h3>
                            <input type="file" id="epub_upload" accept=".epub" style="background: white;">
                            <button id="btn_convert_epub" style="background-color: #10b981;">Convert & Download SRT</button>
                            <div id="epub_status" style="margin-top: 5px; font-weight: bold; font-size: 14px;"></div>
                        </div>

                        <div class="radio-group">
                            <input type="radio" id="src_local" name="source" value="local" checked>
                            <label for="src_local" style="margin-right: 20px;">📁 Local Folder</label>
                            
                            <input type="radio" id="src_abs" name="source" value="abs">
                            <label for="src_abs">📚 Audiobookshelf</label>
                        </div>
                        
                        <div id="sec_local" class="section">
                            <select id="local_selector"><option value="">-- Select local audio --</option></select>
                        </div>
                        
                        <div id="sec_abs" class="section" style="display: none;">
                            <button id="btn_connect_abs">🔌 Connect to Audiobookshelf</button>
                            <span id="abs_status" style="display:block; margin-top:5px; font-weight:bold;"></span>
                            
                            <select id="abs_lib_selector" style="display: none;">
                                <option value="">-- 1. Select a Library --</option>
                            </select>
                            
                            <select id="abs_book_selector" style="display: none; margin-top: 10px;">
                                <option value="">-- 2. Select an Item --</option>
                            </select>
                            
                            <div id="abs_sub_controls" class="sub-controls">
                                <div id="part_container" style="display: none;">
                                    <label id="part_label">📦 Loading...</label>
                                    <select id="abs_part_selector"></select>
                                </div>
                                <div id="chapter_container" style="display: none;">
                                    <label>🔖 Embedded Chapters (Jump to):</label>
                                    <select id="abs_chapter_selector">
                                        <option value="">-- Select a Chapter --</option>
                                    </select>
                                </div>
                            </div>
                        </div>
                        
                        <div style="margin-top: 10px;">
                            <img id="cover_art" src="" alt="Cover Art">
                            <audio id="audio_player" controls style="width: 100%; margin-top: 15px;"></audio>
                        </div>
                        
                        <div id="status">Waiting for selection...</div>
                    </div>
                    
                    <script>
                        const audio = document.getElementById('audio_player');
                        const globalStatus = document.getElementById('status');
                        const coverArt = document.getElementById('cover_art');
                        
                        const radioLocal = document.getElementById('src_local');
                        const radioAbs = document.getElementById('src_abs');
                        const secLocal = document.getElementById('sec_local');
                        const secAbs = document.getElementById('sec_abs');
                        
                        const selLocal = document.getElementById('local_selector');
                        const btnConnect = document.getElementById('btn_connect_abs');
                        const absStatus = document.getElementById('abs_status');
                        const selLib = document.getElementById('abs_lib_selector');
                        const selBook = document.getElementById('abs_book_selector');
                        
                        const subControls = document.getElementById('abs_sub_controls');
                        const partContainer = document.getElementById('part_container');
                        const partLabel = document.getElementById('part_label');
                        const selPart = document.getElementById('abs_part_selector');
                        const chapterContainer = document.getElementById('chapter_container');
                        const selChapter = document.getElementById('abs_chapter_selector');
                        
                        let lastCmdId = '0';
                        let isLoaded = false;
                        let currentLibraryItems = {}; 
                        
                        // ===== EPUB TO SRT FRONTEND LOGIC =====
                        document.getElementById('btn_convert_epub').addEventListener('click', () => {
                            const fileInput = document.getElementById('epub_upload');
                            const statusDiv = document.getElementById('epub_status');
                            
                            if (!fileInput.files.length) {
                                statusDiv.innerText = "❌ Please select an EPUB file first.";
                                statusDiv.style.color = "red";
                                return;
                            }
                            
                            const file = fileInput.files[0];
                            statusDiv.innerText = "⏳ Processing " + file.name + "... Please wait.";
                            statusDiv.style.color = "blue";
                            
                            fetch('/upload_epub?filename=' + encodeURIComponent(file.name), {
                                method: 'POST',
                                body: file
                            })
                            .then(res => {
                                if (!res.ok) {
                                    return res.text().then(text => { throw new Error(text || "Server error"); });
                                }
                                return res.blob();
                            })
                            .then(blob => {
                                // Create a hidden link to trigger the download instantly
                                const url = window.URL.createObjectURL(blob);
                                const a = document.createElement('a');
                                a.href = url;
                                a.download = file.name.replace('.epub', '') + '.srt';
                                document.body.appendChild(a);
                                a.click();
                                a.remove();
                                window.URL.revokeObjectURL(url);
                                
                                statusDiv.innerText = "✅ Subtitles successfully extracted and downloaded!";
                                statusDiv.style.color = "green";
                            })
                            .catch(err => {
                                statusDiv.innerText = "❌ Error: " + err.message;
                                statusDiv.style.color = "red";
                            });
                        });
                        // ======================================
                        
                        function updateSections() {
                            secLocal.style.display = radioLocal.checked ? 'block' : 'none';
                            secAbs.style.display = radioAbs.checked ? 'block' : 'none';
                        }
                        radioLocal.addEventListener('change', updateSections);
                        radioAbs.addEventListener('change', updateSections);
                        
                        fetch('/list_local').then(res => res.json()).then(files => {
                            files.forEach(f => {
                                let opt = document.createElement('option');
                                opt.value = f.url; opt.innerText = f.name;
                                selLocal.appendChild(opt);
                            });
                        });

                        btnConnect.addEventListener('click', () => {
                            absStatus.innerText = "Fetching Libraries...";
                            absStatus.style.color = "blue";
                            
                            fetch('/abs_libraries').then(async res => {
                                let data = await res.json();
                                if (res.ok && data.success) {
                                    absStatus.innerText = `✅ Connected!`;
                                    absStatus.style.color = "green";
                                    btnConnect.style.display = 'none';
                                    selLib.style.display = 'block';
                                    
                                    selLib.innerHTML = '<option value="">-- 1. Select a Library --</option>';
                                    data.libraries.forEach(lib => {
                                        let opt = document.createElement('option');
                                        opt.value = lib.id; opt.innerText = `📚 ${lib.name}`;
                                        selLib.appendChild(opt);
                                    });
                                } else {
                                    absStatus.innerText = "❌ Error: " + (data.error || "Unknown");
                                    absStatus.style.color = "red";
                                }
                            }).catch(() => { absStatus.innerText = "❌ Network Error"; });
                        });

                        selLib.addEventListener('change', (e) => {
                            if (!e.target.value) return;
                            absStatus.innerText = "Fetching Books/Podcasts...";
                            selBook.style.display = 'none';
                            subControls.style.display = 'none';
                            
                            fetch('/abs_items?lib_id=' + e.target.value).then(async res => {
                                let data = await res.json();
                                if (res.ok && data.success) {
                                    absStatus.innerText = `✅ Found ${data.items.length} items.`;
                                    selBook.style.display = 'block';
                                    
                                    selBook.innerHTML = '<option value="">-- 2. Select an Item --</option>';
                                    currentLibraryItems = {}; 
                                    
                                    data.items.forEach(item => {
                                        currentLibraryItems[item.id] = item; 
                                        let opt = document.createElement('option');
                                        opt.value = item.id; 
                                        opt.innerText = item.name;
                                        selBook.appendChild(opt);
                                    });
                                }
                            });
                        });
                        
                        selBook.addEventListener('change', (e) => {
                            let bookId = e.target.value;
                            if (!bookId) {
                                subControls.style.display = 'none';
                                return;
                            }
                            
                            let book = currentLibraryItems[bookId];
                            
                            if (book.cover) {
                                coverArt.src = book.cover;
                                coverArt.style.display = 'block';
                            } else {
                                coverArt.style.display = 'none';
                            }
                            
                            subControls.style.display = 'block';
                            partContainer.style.display = 'block';
                            chapterContainer.style.display = 'none';
                            
                            partLabel.innerText = "⏳ Querying Server for Episodes/Parts...";
                            selPart.innerHTML = '';
                            
                            fetch('/abs_item_details?id=' + bookId)
                                .then(res => res.json())
                                .then(data => {
                                    if(data.success) {
                                        let isPodcast = data.mediaType === 'podcast' || data.parts.some(p => p.type === 'episode');
                                        partLabel.innerText = isPodcast ? '🎙️ Select an Episode:' : '📦 Physical Files / Parts:';
                                        
                                        selPart.innerHTML = '<option value="">-- Select --</option>';
                                        
                                        if (data.parts && data.parts.length > 0) {
                                            data.parts.forEach(part => {
                                                let opt = document.createElement('option');
                                                opt.value = JSON.stringify(part);
                                                opt.innerText = part.name;
                                                selPart.appendChild(opt);
                                            });
                                        } else {
                                            selPart.innerHTML = '<option value="">(No Playable Files Found)</option>';
                                        }
                                        
                                        if (data.chapters && data.chapters.length > 0) {
                                            chapterContainer.style.display = 'block';
                                            selChapter.innerHTML = '<option value="">-- Select a Chapter --</option>';
                                            data.chapters.forEach(ch => {
                                                let opt = document.createElement('option');
                                                opt.value = ch.start; 
                                                opt.innerText = ch.title + ` (${new Date(ch.start * 1000).toISOString().substr(11, 8)})`;
                                                selChapter.appendChild(opt);
                                            });
                                        }
                                        
                                        if (data.parts.length === 1 && !isPodcast) {
                                            selPart.selectedIndex = 1; 
                                            triggerAbsDownload(bookId, JSON.parse(selPart.value));
                                        }
                                    } else {
                                        partLabel.innerText = "❌ Error loading item details.";
                                    }
                                })
                                .catch(err => {
                                    partLabel.innerText = "❌ Network Error.";
                                });
                        });
                        
                        selPart.addEventListener('change', (e) => {
                            if(!e.target.value) return;
                            
                            let partData = JSON.parse(e.target.value);
                            if (partData.type === 'not_downloaded') {
                                globalStatus.innerText = "❌ This episode hasn't been downloaded to your Audiobookshelf server yet!";
                                globalStatus.style.color = "red";
                                return;
                            }
                            
                            triggerAbsDownload(selBook.value, partData);
                        });
                        
                        selChapter.addEventListener('change', (e) => {
                            if(!e.target.value || !isLoaded) return;
                            let targetTime = parseFloat(e.target.value);
                            audio.currentTime = targetTime;
                            audio.play(); 
                            globalStatus.innerText = "✅ Jumped to Chapter!";
                        });

                        function triggerAbsDownload(bookId, partData) {
                            isLoaded = false;
                            audio.src = '';
                            
                            globalStatus.innerText = "⏳ Caching from Audiobookshelf... (Please wait)";
                            globalStatus.style.color = "#007bff";
                            
                            let fetchUrl = `/stream_abs?id=${bookId}&type=${partData.type}`;
                            if (partData.ino) fetchUrl += `&ino=${partData.ino}`;
                            if (partData.ep_id) fetchUrl += `&ep_id=${partData.ep_id}`;
                            
                            fetch(fetchUrl)
                                .then(res => res.json())
                                .then(data => {
                                    if (data.success) {
                                        audio.src = data.local_url;
                                        audio.oncanplay = () => {
                                            globalStatus.innerText = "✅ Stream Ready! Progress bar & +30s now fully unlocked.";
                                            globalStatus.style.color = "green";
                                            isLoaded = true;
                                        };
                                        audio.onerror = () => {
                                            globalStatus.innerText = "❌ Error: Format rejected by browser.";
                                            globalStatus.style.color = "red";
                                        };
                                    } else {
                                        globalStatus.innerText = "❌ Error: " + data.error;
                                        globalStatus.style.color = "red";
                                    }
                                })
                                .catch(err => {
                                    globalStatus.innerText = "❌ Network Error.";
                                    globalStatus.style.color = "red";
                                });
                        }
                        
                        selLocal.addEventListener('change', (e) => {
                            if (!e.target.value) return;
                            isLoaded = false;
                            audio.src = '';
                            coverArt.style.display = 'none';
                            subControls.style.display = 'none';
                            
                            globalStatus.innerText = "Loading local file...";
                            globalStatus.style.color = "#333";
                            
                            audio.src = e.target.value;
                            audio.oncanplay = () => {
                                globalStatus.innerText = "✅ Local Audio Ready! Waiting for Kindle...";
                                globalStatus.style.color = "green";
                                isLoaded = true;
                            };
                        });
                        
                        setInterval(async () => {
                            if (!isLoaded) return; 
                            try {
                                let res = await fetch('/state?t=' + new Date().getTime());
                                let data = await res.json();
                                
                                if (data.command && data.command.id !== lastCmdId) {
                                    lastCmdId = data.command.id;
                                    let action = data.command.action;
                                    let val = parseFloat(data.command.val || 0);
                                    
                                    if (action === 'play') audio.play().catch(() => {});
                                    else if (action === 'pause') audio.pause();
                                    else if (action === 'seek_relative') {
                                        audio.currentTime = Math.max(0, Math.min(audio.duration, audio.currentTime + val));
                                    }
                                    else if (action === 'set_time') audio.currentTime = Math.max(0, Math.min(audio.duration, val));
                                    else if (action === 'speed') audio.playbackRate = val;
                                }

                                let stat = audio.paused ? '⏸ PAUSED' : '▶ PLAYING';
                                globalStatus.innerText = `[ ${stat} ]  Time: ${Math.floor(audio.currentTime)}s  |  Speed: ${audio.playbackRate}x`;
                                globalStatus.style.color = audio.paused ? '#d35400' : 'green';
                                
                                fetch(`/heartbeat?time=${audio.currentTime}&duration=${audio.duration||0}&status=${audio.paused?'pause':'play'}&speed=${audio.playbackRate}&t=${new Date().getTime()}`);
                            } catch (e) {}
                        }, 500);
                    </script>
                </body>
                </html>
                """
                self.wfile.write(html.encode())

            # --------------------------------------------------------
            # API: LOCAL FILES
            # --------------------------------------------------------
            elif parsed.path == '/list_local':
                items = []
                if os.path.exists(AUDIO_DIR):
                    exts = ('.mp3', '.m4a', '.wav', '.ogg', '.flac', '.m4b')
                    for f in sorted(os.listdir(AUDIO_DIR)):
                        if f.lower().endswith(exts):
                            items.append({"name": f, "url": f"audios/{f}"})
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(items).encode())
                
            # --------------------------------------------------------
            # API: ABS LIBRARIES
            # --------------------------------------------------------
            elif parsed.path == '/abs_libraries':
                if not ABS_URL or not ABS_TOKEN or ABS_TOKEN == "PASTE_YOUR_TOKEN_HERE":
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"success": False, "error": "Check config.json for Token/URL"}).encode())
                    return
                    
                try:
                    req = urllib.request.Request(f"{ABS_URL}/api/libraries", headers={"Authorization": f"Bearer {ABS_TOKEN}"})
                    with urllib.request.urlopen(req, timeout=10) as response:
                        libraries = json.loads(response.read().decode()).get('libraries', [])
                        
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"success": True, "libraries": libraries}).encode())
                except Exception as e:
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode())

            # --------------------------------------------------------
            # API: ABS ITEMS (LIGHTWEIGHT FOR FAST LOADING)
            # --------------------------------------------------------
            elif parsed.path == '/abs_items':
                lib_id = parse_qs(parsed.query).get('lib_id', [''])[0]
                if not lib_id:
                    self.send_response(400)
                    self.end_headers()
                    return
                
                try:
                    req_items = urllib.request.Request(f"{ABS_URL}/api/libraries/{lib_id}/items?limit=2000", headers={"Authorization": f"Bearer {ABS_TOKEN}"})
                    with urllib.request.urlopen(req_items, timeout=10) as res_items:
                        results = json.loads(res_items.read().decode()).get('results', [])
                        
                        items = []
                        for item in results:
                            meta = item.get('media', {}).get('metadata', {}) or item.get('metadata', {})
                            title = meta.get('title', 'Unknown Title')
                            author = meta.get('authorName', '')
                            item_id = item.get('id')
                            
                            display_name = f"🎧 {title}" + (f" ({author})" if author else "")
                            cover_url = f"{ABS_URL}/api/items/{item_id}/cover?token={ABS_TOKEN}"
                            
                            items.append({
                                "id": item_id,
                                "name": display_name,
                                "cover": cover_url
                            })
                                    
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"success": True, "items": items}).encode())
                    
                except Exception as e:
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode())

            # --------------------------------------------------------
            # API: ABS ITEM DETAILS (THE FIX FOR PODCAST EPISODES)
            # --------------------------------------------------------
            elif parsed.path == '/abs_item_details':
                item_id = parse_qs(parsed.query).get('id', [''])[0]
                if not item_id:
                    self.send_response(400)
                    self.end_headers()
                    return
                
                try:
                    req_item = urllib.request.Request(f"{ABS_URL}/api/items/{item_id}", headers={"Authorization": f"Bearer {ABS_TOKEN}"})
                    with urllib.request.urlopen(req_item, timeout=10) as res_item:
                        item = json.loads(res_item.read().decode())
                        
                    media = item.get('media', {})
                    media_type = item.get('mediaType', '')
                    
                    parts = []
                    chapters = []
                    
                    audio_files = media.get('audioFiles', []) or media.get('tracks', [])
                    episodes = media.get('episodes', [])
                    
                    if media_type == 'podcast' or episodes:
                        for ep in episodes:
                            if 'id' in ep:
                                ep_title = ep.get('title') or ep.get('subtitle') or f"Episode {ep.get('season', '')}x{ep.get('episode', '')}"
                                if ep_title.strip() == "x": ep_title = "Unknown Episode"
                                
                                published = ep.get('publishedYear')
                                date_str = f" ({published})" if published else ""
                                
                                # CRITICAL FIX: Find the actual physical file ID (ino)
                                audio_file = ep.get('audioFile') or ep.get('audioTrack') or {}
                                ino = audio_file.get('ino')
                                
                                if ino:
                                    # The episode is physically downloaded! Route it natively like a standard audiobook file.
                                    parts.append({
                                        "type": "file",
                                        "ino": ino,
                                        "name": f"🎙️ {ep_title}{date_str}"
                                    })
                                else:
                                    # It's just an RSS placeholder, not actually downloaded!
                                    parts.append({
                                        "type": "not_downloaded",
                                        "ep_id": ep['id'],
                                        "name": f"☁️ {ep_title}{date_str} (Not Downloaded)"
                                    })
                    elif audio_files:
                        for i, af in enumerate(audio_files):
                            filename = af.get('metadata', {}).get('filename', f"Part {i+1}")
                            parts.append({
                                "type": "file",
                                "ino": af.get('ino', ''),
                                "name": f"File: {filename}"
                            })
                    else:
                        parts.append({
                            "type": "download",
                            "name": "Full Download"
                        })
                        
                    for idx, ch in enumerate(media.get('chapters', [])):
                        chapters.append({
                            "id": ch.get('id', idx),
                            "title": ch.get('title', f"Chapter {idx+1}"),
                            "start": ch.get('start', 0)
                        })
                        
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "success": True, 
                        "parts": parts, 
                        "chapters": chapters, 
                        "mediaType": media_type
                    }).encode())
                    
                except Exception as e:
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode())

            # --------------------------------------------------------
            # API: PROXY CACHE MATCHER (FIXED FOR EPISODES)
            # --------------------------------------------------------
            elif parsed.path == '/stream_abs':
                item_id = parse_qs(parsed.query).get('id', [''])[0]
                dl_type = parse_qs(parsed.query).get('type', [''])[0]
                ino = parse_qs(parsed.query).get('ino', [''])[0]
                ep_id = parse_qs(parsed.query).get('ep_id', [''])[0]
                
                if not item_id:
                    self.send_response(400)
                    self.end_headers()
                    return
                
                # Because we now map downloaded episodes to their native 'ino' file ID, 
                # we naturally hit the most stable Audiobookshelf endpoint!
                if dl_type == "episode" and ep_id:
                    download_url = f"{ABS_URL}/api/items/{item_id}/episodes/{ep_id}/download?token={ABS_TOKEN}"
                    cache_prefix = f"abs_{item_id}_ep_{ep_id}"
                elif dl_type == "file" and ino:
                    download_url = f"{ABS_URL}/api/items/{item_id}/file/{ino}?token={ABS_TOKEN}"
                    cache_prefix = f"abs_{item_id}_ino_{ino}"
                else:
                    download_url = f"{ABS_URL}/api/items/{item_id}/download?token={ABS_TOKEN}"
                    cache_prefix = f"abs_{item_id}_dl"
                
                existing_files = glob.glob(os.path.join(AUDIO_DIR, f"{cache_prefix}.*"))
                
                if existing_files:
                    temp_filename = os.path.basename(existing_files[0])
                    print(f"\n[ABS] Part already cached locally as {temp_filename}! Loading instantly.")
                else:
                    print(f"\n[ABS] Proxying audio part from Audiobookshelf...")
                    try:
                        req = urllib.request.Request(download_url)
                        with urllib.request.urlopen(req, timeout=30) as response:
                            filename = response.info().get_filename()
                            ext = ".mp3" 
                            
                            if filename:
                                _, ext = os.path.splitext(filename)
                                ext = ext.lower()
                            else:
                                ctype = response.info().get_content_type()
                                if 'mp4' in ctype or 'm4b' in ctype or 'm4a' in ctype: ext = '.m4b'
                            
                            if ext == '.zip':
                                print(f"[ABS] ZIP archive detected! Extracting the specific M4B/MP3...")
                                temp_zip_filename = f"{cache_prefix}_temp.zip"
                                temp_zip_filepath = os.path.join(AUDIO_DIR, temp_zip_filename)
                                
                                with open(temp_zip_filepath, 'wb') as out_file:
                                    shutil.copyfileobj(response, out_file)
                                
                                extract_dir = os.path.join(AUDIO_DIR, f"{cache_prefix}_unzipped")
                                os.makedirs(extract_dir, exist_ok=True)
                                
                                with zipfile.ZipFile(temp_zip_filepath, 'r') as zip_ref:
                                    zip_ref.extractall(extract_dir)
                                    
                                extracted_audios = []
                                for root, dirs, files in os.walk(extract_dir):
                                    for f in files:
                                        if f.lower().endswith(('.mp3', '.m4a', '.m4b', '.wav', '.ogg', '.flac')):
                                            extracted_audios.append(os.path.join(root, f))
                                            
                                if not extracted_audios: raise Exception("No audio files inside the ZIP.")
                                    
                                extracted_audios.sort()
                                target_file = extracted_audios[0] 
                                _, final_ext = os.path.splitext(target_file)
                                
                                temp_filename = f"{cache_prefix}{final_ext.lower()}"
                                final_filepath = os.path.join(AUDIO_DIR, temp_filename)
                                shutil.move(target_file, final_filepath)
                                
                                try:
                                    shutil.rmtree(extract_dir)
                                    os.remove(temp_zip_filepath)
                                except: pass
                                
                            else:
                                temp_filename = f"{cache_prefix}{ext}"
                                temp_filepath = os.path.join(AUDIO_DIR, temp_filename)
                                with open(temp_filepath, 'wb') as out_file:
                                    shutil.copyfileobj(response, out_file)
                                print(f"[ABS] Download complete! Saved natively as {temp_filename}")
                                
                    except Exception as e:
                        print(f"[ABS ERROR] {e}")
                        self.send_response(200)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode())
                        return
                
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": True, 
                    "local_url": f"/audios/{temp_filename}"
                }).encode())

            # --------------------------------------------------------
            # API: NATIVE RANGE REQUEST STREAMER
            # --------------------------------------------------------
            elif parsed.path.startswith('/audios/'):
                filepath = parsed.path.lstrip('/')
                if not os.path.exists(filepath):
                    self.send_error(404, "File not found")
                    return
                
                file_size = os.path.getsize(filepath)
                mtype, _ = mimetypes.guess_type(filepath)
                
                if 'Range' in self.headers:
                    range_header = self.headers['Range'].replace('bytes=', '').split('-')
                    start = int(range_header[0]) if range_header[0] else 0
                    end = int(range_header[1]) if len(range_header) > 1 and range_header[1] else file_size - 1
                    length = end - start + 1
                    
                    self.send_response(206)
                    self.send_header('Content-type', mtype or 'application/octet-stream')
                    self.send_header('Accept-Ranges', 'bytes')
                    self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
                    self.send_header('Content-Length', str(length))
                    self.end_headers()
                    
                    with open(filepath, 'rb') as f:
                        f.seek(start)
                        bytes_to_read = length
                        while bytes_to_read > 0:
                            chunk = f.read(min(8192, bytes_to_read))
                            if not chunk: break
                            self.wfile.write(chunk)
                            bytes_to_read -= len(chunk)
                else:
                    self.send_response(200)
                    self.send_header('Content-type', mtype or 'application/octet-stream')
                    self.send_header('Accept-Ranges', 'bytes')
                    self.send_header('Content-Length', str(file_size))
                    self.end_headers()
                    with open(filepath, 'rb') as f:
                        shutil.copyfileobj(f, self.wfile)

            # --------------------------------------------------------
            # KOREADER COMMAND ROUTES
            # --------------------------------------------------------
            elif parsed.path == '/cmd':
                qs = parse_qs(parsed.query)
                state["command"] = {
                    "action": qs.get('action', ['none'])[0],
                    "val": float(qs.get('val', [0])[0]),
                    "id": qs.get('id', ['0'])[0]
                }
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
                
            elif parsed.path == '/heartbeat':
                qs = parse_qs(parsed.query)
                state["browser"]["time"] = float(qs.get('time', [0])[0])
                state["browser"]["duration"] = float(qs.get('duration', [0])[0])
                state["browser"]["status"] = qs.get('status', ['pause'])[0]
                state["browser"]["speed"] = float(qs.get('speed', [1.0])[0])
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
                
            elif parsed.path == '/state':
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.end_headers()
                self.wfile.write(json.dumps(state).encode())
                
            else:
                super().do_GET()
                
        except (BrokenPipeError, ConnectionResetError):
            pass

if __name__ == '__main__':
    load_config()
    with http.server.ThreadingHTTPServer(("", PORT), AudioHandler) as httpd:
        print(f"Server running! Open your browser to http://localhost:{PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")
            sys.exit(0)
